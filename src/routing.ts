/**
 * Who gets a round, and when to look at it again. See DESIGN.md §3.2 and §3.5.
 *
 * Pure arithmetic over peers and deadlines — no bytes, no sockets, no clock of
 * its own, and no import at all. The bytes are wire.ts; relay.ts is what feeds
 * this its peers and acts on the answer.
 */

/** How long a pipe stays reserved after its last message. */
export const pipeIdleMs = 2_000

/** One connected device of an owner. `id` increases with connection order. */
export interface Peer {
  readonly id: number
  /** The peer this one is piping with, or null when free. */
  readonly partnerId: number | null
  /** When that pipe last carried a message. */
  readonly pairedAt: number
}

export interface Route {
  /** Gets the round as a Response, and answers with the diff. */
  readonly partnerId: number | null
  /** Get the same body as a Broadcast. */
  readonly listenerIds: ReadonlyArray<number>
  /** Nobody left to sync with: tell the sender it is in sync, drop the round. */
  readonly inSync: boolean
  /** True when `inSync` is the end of a sweep, not a device sitting alone. */
  readonly exhausted: boolean
  /**
   * No partner right now. Keep the round and route it again once a pipe has
   * gone quiet — `pipeIdleMs` from the newest `pairedAt` is when that is.
   */
  readonly hold: boolean
}

/**
 * Who receives a round. A socket is a single channel with no conversation id,
 * so a peer already piping with someone else must not be handed a second
 * conversation — it stays reserved until its pipe has been quiet for
 * {@link pipeIdleMs}, which is checked here rather than by a timer so the
 * Durable Object can hibernate between messages.
 *
 * Among free peers the **newest** wins. A device whose network dropped leaves a
 * socket that is still open here and will never answer again; after the outage
 * both devices reconnect, and picking the newest socket pairs those two fresh
 * connections with each other instead of handing the round to the leftover,
 * where the answer would be lost and sync would look dead.
 *
 * Introductions are serialized per owner: when `isJoin` is a socket's first
 * round, it waits until every pipe of this owner has gone quiet. Whoever then
 * answers it has already ingested the previous newcomer's data, which is what
 * makes "every connected device holds the union" true when several devices
 * connect at once. Later rounds from that socket never wait.
 *
 * `triedIds` are the peers this round has already been handed and that did not
 * answer within {@link answerTimeoutMs}. They are never offered it again, so a
 * round sweeps the peers one at a time. When the sweep runs out, the sender is
 * told it is in sync (`exhausted`) — the same answer a device connected alone
 * gets, because operationally it is the same situation. It may be behind, and
 * the next introduction by any peer repairs it: reconciliation is two-way, so
 * whoever joins next hands it what it lacks.
 */
export const routeFor = (
  senderId: number,
  peers: ReadonlyArray<Peer>,
  now: number,
  { isJoin = false, triedIds = [] }: RouteOptions = {}
): Route => {
  const ordered = [...peers].sort((a, b) => a.id - b.id)
  const sender = ordered.find((peer) => peer.id === senderId)
  const others = ordered.filter((peer) => peer.id !== senderId)

  if (others.length === 0) {
    return {
      partnerId: null,
      listenerIds: [],
      inSync: true,
      exhausted: false,
      hold: false,
    }
  }

  const candidates = others.filter((peer) => !triedIds.includes(peer.id))

  if (candidates.length === 0) {
    return {
      partnerId: null,
      listenerIds: [],
      inSync: true,
      exhausted: true,
      hold: false,
    }
  }

  const isFree = (peer: Peer) =>
    peer.partnerId === null || now - peer.pairedAt >= pipeIdleMs

  const listenersOnly = (): Route => ({
    partnerId: null,
    listenerIds: others.map((peer) => peer.id),
    inSync: false,
    exhausted: false,
    hold: true,
  })

  if (isJoin && others.some((peer) => !isFree(peer))) return listenersOnly()

  const running =
    sender !== undefined && !isFree(sender)
      ? (candidates.find((peer) => peer.id === sender.partnerId) ?? null)
      : null

  const partner = running ?? candidates.findLast(isFree) ?? null
  if (partner === null) return listenersOnly()

  return {
    partnerId: partner.id,
    listenerIds: others
      .filter((peer) => peer.id !== partner.id)
      .map((peer) => peer.id),
    inSync: false,
    exhausted: false,
    hold: false,
  }
}

export interface RouteOptions {
  /** This is the socket's first round for the owner. */
  readonly isJoin?: boolean
  /** Peers that were handed this round and did not answer. */
  readonly triedIds?: ReadonlyArray<number>
}

/**
 * How long a partner has to answer a round before it is offered to the next
 * peer. Shorter than {@link pipeIdleMs}, so a sweep makes progress before the
 * pipe itself goes stale, but long enough for a phone to fingerprint its
 * database. An answer that arrives later still delivers its data, as a
 * Broadcast — the sender is busy with its new partner by then, so it can never
 * be handed a second conversation on one socket.
 */
export const answerTimeoutMs = 800

/** A round waiting for a partner, or for the partner's answer. */
export interface PendingRound {
  readonly senderId: number
  readonly message: Uint8Array
  readonly isJoin: boolean
  /** Peers already handed this round. */
  readonly triedIds: ReadonlyArray<number>
  /** When the current partner's answer is due; null while waiting for one. */
  readonly deadline: number | null
}

/** ponytail: fixed cap, revisit if a real device ever fills it. */
const maxHeldRounds = 16

export const addPending = (
  pending: Array<PendingRound>,
  round: PendingRound
): void => {
  if (pending.length >= maxHeldRounds) pending.shift()
  pending.push(round)
}

/**
 * The partner answered, so the round it owed an answer to is done.
 *
 * A sender can have two rounds here at once — one still held for want of a
 * partner, one handed to one — and the answer belongs to the second, so that
 * is the one this ends. Taking the held one instead would leave the answered
 * round to time out, rip up a running pipe and re-offer a stale body to the
 * next peer.
 */
export const resolvePending = (
  pending: Array<PendingRound>,
  senderId: number
): void => {
  const isFrom = (round: PendingRound) => round.senderId === senderId
  const awaiting = pending.findIndex(
    (round) => isFrom(round) && round.deadline !== null
  )
  const index = awaiting === -1 ? pending.findIndex(isFrom) : awaiting
  if (index !== -1) pending.splice(index, 1)
}

/** Rounds whose partner ran out of time, or that never got one. */
export const duePending = (
  pending: Array<PendingRound>,
  now: number
): Array<PendingRound> => {
  const due = pending.filter(
    (round) => round.deadline === null || round.deadline <= now
  )
  for (const round of due) {
    const index = pending.indexOf(round)
    if (index !== -1) pending.splice(index, 1)
  }
  return due
}

/**
 * When the pending rounds are worth looking at again: the earliest of a
 * partner's answer running out of time and a running pipe falling quiet. Null
 * when there is nothing to wait for. The relay hands it to its host, which is
 * what owns a clock.
 */
export const nextWakeMs = (
  peers: ReadonlyArray<Peer>,
  pending: ReadonlyArray<PendingRound>,
  now: number
): number | null => {
  if (pending.length === 0) return null

  const deadlines = pending
    .map((round) => round.deadline)
    .filter((deadline) => deadline !== null)
    .map((deadline) => deadline - now)

  const quiet = peers
    .filter((peer) => peer.partnerId !== null)
    .map((peer) => peer.pairedAt + pipeIdleMs - now)

  const waits = [...deadlines, ...quiet].filter((delay) => delay > 0)

  return waits.length === 0 ? 0 : Math.min(...waits)
}
