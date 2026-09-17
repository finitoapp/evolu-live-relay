import type { Console, Id } from "@evolu/common"
import {
  MessageType,
  type OwnerId,
  ProtocolErrorCode,
  parseProtocolHeader,
} from "@evolu/common/local-first"
import {
  addPending,
  answerTimeoutMs,
  duePending,
  nextWakeMs,
  type Peer,
  type PendingRound,
  resolvePending,
  routeFor,
} from "./routing.ts"
import {
  inspect,
  isUnsubscribe,
  readOwnerId,
  readWriteKey,
  requestBodyOffset,
  responseWithoutBody,
  toBroadcast,
  toResponse,
} from "./wire.ts"

/**
 * The relay itself: one owner's devices, and the rounds moving between them.
 * See DESIGN.md §4.
 *
 * This is the whole program, and it is written once. It knows nothing about
 * sockets, servers, timers or hosting — it asks a {@link RelayHost} for the
 * sockets of its owner, for the time, and for a wake-up, and it reads and
 * writes each socket's state through a {@link RelaySocket}. What is left in
 * server.ts and worker.ts is the adapter that supplies those.
 *
 * Below it are the two halves it is made of, neither of which holds state:
 * wire.ts, what a round looks like on the wire, and routing.ts, who gets one.
 *
 * One instance per room, in both hosts: in the Durable Object that is the
 * object itself, locally it is an entry in a map. A room is one owner, but the
 * relay is not told which — it learns that from the first round it is given,
 * the same way it learns the write key, because the address says nothing about
 * it and every message carries it anyway. Nothing here is persisted —
 * the rounds waiting for a partner live in memory on purpose, because writing
 * them down would make this a database (DESIGN.md §6).
 */

/**
 * What the relay remembers about one socket. A superset of {@link Peer}, which
 * is all `routeFor` needs to see.
 *
 * It is small and serializable because a Durable Object keeps it in the
 * socket's attachment, the only thing that survives hibernation.
 */
export interface SocketState extends Peer {
  /** False until this socket's first round, which is its introduction. */
  readonly joined: boolean
  /** Set by an Unsubscribe round or a close; the socket stops being a peer. */
  readonly left: boolean
}

/** The state a socket starts with. `id` has to increase with connection order. */
export const initialSocketState = (id: number): SocketState => ({
  id,
  partnerId: null,
  pairedAt: 0,
  joined: false,
  left: false,
})

/** One connected socket, as the relay sees it. */
export interface RelaySocket {
  readonly send: (bytes: Uint8Array) => void
  readonly state: () => SocketState
  readonly setState: (next: SocketState) => void
}

/** What the relay needs from whoever is hosting it. */
export interface RelayHost {
  /** Every open socket the host holds for this owner, in any order. */
  readonly openSockets: () => ReadonlyArray<RelaySocket>
  /**
   * Call {@link Relay.wake} in `delayMs`, replacing any wake already pending;
   * null cancels it. `setTimeout` locally, `setAlarm` in a Durable Object.
   */
  readonly wakeAt: (delayMs: number | null) => void
  readonly now: () => number
  /**
   * Evolu's `Console`, from the Run at the host's composition root. The relay
   * takes a `child` of it per owner, so every line it writes is already
   * prefixed with whose it is, and a host can turn the per-round chatter off
   * with `setLevel` without touching this code.
   */
  readonly console: Console
}

export interface Relay {
  /** One inbound frame from a socket of this owner. */
  readonly receive: (socket: RelaySocket, message: Uint8Array) => void
  /** That socket is gone. */
  readonly disconnect: (socket: RelaySocket) => void
  /** The wake the host was asked for. */
  readonly wake: () => void
  /** Nothing is connected and nothing is held: the host may drop this relay. */
  readonly isIdle: () => boolean
}

export const createRelay = (roomId: Id, host: RelayHost): Relay => {
  /**
   * Rounds waiting for a partner, or for a partner's answer. In memory on
   * purpose; a Durable Object may lose them to hibernation, and the device
   * then catches up on its next sync — see DESIGN.md §6.
   */
  const pending: Array<PendingRound> = []

  /** First write key seen while this room has connections. */
  let ownerWriteKey: string | null = null

  /**
   * The owner this room settled on, from its first round. Reaching a room
   * takes the owner's secret (DESIGN.md §4b), so this is not a gate against an
   * attacker — it is what turns an app pointing two owners at one room into a
   * visible drop rather than silently mixed data.
   */
  let roomOwnerId: OwnerId | null = null

  /**
   * Shadows the global `console` on purpose: inside this file there is no
   * other one, and a stray global call would be a dependency this layer is
   * not allowed to have.
   */
  const console = host.console.child(roomId)

  const patch = (socket: RelaySocket, next: Partial<SocketState>) => {
    socket.setState({ ...socket.state(), ...next })
  }

  /**
   * Sockets that count: introduced, not gone. A socket that has connected but
   * never synced is not one yet, and neither is one whose write key was
   * refused, so neither can be handed another device's round.
   */
  const peerSockets = (): ReadonlyArray<RelaySocket> =>
    host.openSockets().filter((socket) => {
      const state = socket.state()
      return state.joined && !state.left
    })

  const byId = (sockets: ReadonlyArray<RelaySocket>) =>
    new Map(sockets.map((socket) => [socket.state().id, socket]))

  /** Ends a socket's pipe, on both sides. Addressed by id, never by identity. */
  const unpair = (socketId: number) => {
    for (const socket of host.openSockets()) {
      const state = socket.state()
      if (state.id === socketId || state.partnerId === socketId) {
        patch(socket, { partnerId: null, pairedAt: 0 })
      }
    }
  }

  const dropPendingFrom = (socketId: number) => {
    const kept = pending.filter((round) => round.senderId !== socketId)
    pending.splice(0, pending.length, ...kept)
  }

  /** Asks the host for the next wake worth having. */
  const scheduleWake = () => {
    const wake = nextWakeMs(
      peerSockets().map((socket) => socket.state()),
      pending,
      host.now()
    )
    host.wakeAt(wake === null ? null : Math.max(wake, 50))
  }

  const route = (
    from: RelaySocket,
    message: Uint8Array,
    isJoin: boolean,
    triedIds: ReadonlyArray<number> = []
  ) => {
    // The header is not parsed again here: `receive` has already validated it,
    // and `parseProtocolHeader` copies the whole frame. Where the owner is
    // needed it is read straight out of the bytes instead.
    const bodyOffset = requestBodyOffset(message)

    const self = from.state()
    const peers = peerSockets()
    const decision = routeFor(
      self.id,
      peers.map((socket) => socket.state()),
      host.now(),
      { isJoin, triedIds }
    )

    if (decision.inSync) {
      // Either nobody is here, or nobody answered: same answer either way. The
      // device may be behind, and the next introduction by any peer repairs it,
      // because reconciliation is two-way.
      from.send(
        responseWithoutBody(readOwnerId(message), ProtocolErrorCode.NoError)
      )
      console.debug(decision.exhausted ? "gave-up" : "alone", {
        socket: self.id,
        tried: triedIds.length,
      })
      return
    }

    const socketsById = byId(peers)
    const shape = inspect(message, bodyOffset)
    const carriesData = !shape.ok || shape.value.messageCount > 0

    // A Broadcast's ranges are ignored by the client, so the whole body can be
    // forwarded untouched — but a round with no messages has nothing to tell it.
    const broadcastTo = (ids: ReadonlyArray<number>) => {
      if (!carriesData || ids.length === 0) return
      const broadcast = toBroadcast(message, bodyOffset)
      for (const id of ids) socketsById.get(id)?.send(broadcast)
    }

    // No ranges means the sender wants no further round: a mutation push, or the
    // last word of a conversation. Nothing to pipe, and the pipe is over.
    if (shape.ok && shape.value.rangeCount === 0) {
      unpair(self.id)
      resolvePending(pending, self.id)
      broadcastTo(
        peers.map((socket) => socket.state().id).filter((id) => id !== self.id)
      )
      console.info("done", {
        socket: self.id,
        messages: shape.value.messageCount,
      })
      scheduleWake()
      return
    }

    broadcastTo(decision.listenerIds)

    const partner =
      decision.partnerId === null
        ? undefined
        : socketsById.get(decision.partnerId)

    if (decision.hold || partner === undefined) {
      // Either every peer is busy, or this is an introduction and another one is
      // still running. Hold the round — the client has no timeout, it just
      // processes whatever arrives whenever it arrives.
      addPending(pending, {
        senderId: self.id,
        message,
        isJoin,
        triedIds,
        deadline: null,
      })
      console.debug("held", { socket: self.id, depth: pending.length })
      scheduleWake()
      return
    }

    partner.send(toResponse(message, bodyOffset))

    const partnerId = partner.state().id
    const pairedAt = host.now()
    patch(from, { partnerId, pairedAt })
    patch(partner, { partnerId: self.id, pairedAt })

    // Wait for that partner's answer; when it does not come, the round is
    // offered to the next peer that has not had it.
    addPending(pending, {
      senderId: self.id,
      message,
      isJoin,
      triedIds: [...triedIds, partnerId],
      deadline: pairedAt + answerTimeoutMs,
    })
    console.info("pipe", { from: self.id, to: partnerId })
    scheduleWake()
  }

  /** An Unsubscribe round or a closed socket: it stops being a peer. */
  const leave = (socket: RelaySocket, reason: "unsubscribed" | "closed") => {
    const { id } = socket.state()
    patch(socket, {
      partnerId: null,
      pairedAt: 0,
      joined: false,
      left: true,
    })
    unpair(id)
    dropPendingFrom(id)
    console.info("leave", { socket: id, peers: peerSockets().length, reason })
    scheduleWake()
  }

  const receive = (socket: RelaySocket, message: Uint8Array) => {
    const { id } = socket.state()

    const header = parseProtocolHeader(message)
    if (!header.ok) {
      console.debug("drop", { socket: id, reason: "invalid-header" })
      return
    }

    if (header.value.messageType !== MessageType.Request) {
      console.debug("drop", {
        socket: id,
        reason: `message-type-${header.value.messageType}`,
      })
      return
    }

    // The first round settles whose room this is; everything after it has to
    // agree, so a socket cannot smuggle a second owner through.
    roomOwnerId ??= header.value.ownerId
    if (header.value.ownerId !== roomOwnerId) {
      console.debug("drop", { socket: id, reason: "foreign-owner" })
      return
    }

    const bodyOffset = requestBodyOffset(message)
    if (bodyOffset > message.length) {
      console.debug("drop", { socket: id, reason: "truncated-header" })
      return
    }

    // Checked before the socket joins, so a rejected one never becomes a peer.
    // ponytail: plain equality, not a timing-safe compare — this only keeps two
    // unrelated owners apart, the payload stays end-to-end encrypted regardless.
    const roundWriteKey = readWriteKey(message)
    const shape = inspect(message, bodyOffset)
    const carriesData = !shape.ok || shape.value.messageCount > 0

    // Not a drop: the relay carries on as if it had not looked. It is worth a
    // line because a reader that gave up is how a wire-format drift shows up
    // in production — see the ponytail note on `inspect`.
    if (!shape.ok) {
      console.warn("unreadable-round", {
        socket: id,
        reason: shape.error.reason,
      })
    }

    if (roundWriteKey !== null) ownerWriteKey ??= roundWriteKey

    // Stricter than a relay: the key is what keeps an unrelated connection out of
    // an owner's pipe group, so a mismatch is refused, and so is a round that
    // writes without one.
    const keyRefused =
      roundWriteKey === null ? carriesData : ownerWriteKey !== roundWriteKey

    if (keyRefused) {
      console.warn("drop", { socket: id, reason: "write-key" })
      socket.send(
        responseWithoutBody(
          header.value.ownerId,
          ProtocolErrorCode.WriteKeyError
        )
      )
      return
    }

    const self = socket.state()

    // A round from a piped peer is the answer the other side was waiting for.
    if (self.partnerId !== null) resolvePending(pending, self.partnerId)

    const isJoin = !self.joined
    if (isJoin || self.left) patch(socket, { joined: true, left: false })
    if (isJoin)
      console.info("join", { socket: id, peers: peerSockets().length })

    if (isUnsubscribe(message, bodyOffset)) {
      leave(socket, "unsubscribed")
      return
    }

    route(socket, message, isJoin)
  }

  /** A partner ran out of time, or a held round can move: route them again. */
  const wake = () => {
    const sockets = byId(peerSockets())

    for (const round of duePending(pending, host.now())) {
      const socket = sockets.get(round.senderId)
      if (socket === undefined) continue

      // The partner it was handed to never answered, so that pipe is over.
      if (round.deadline !== null) unpair(round.senderId)

      route(socket, round.message, round.isJoin, round.triedIds)
    }

    scheduleWake()
  }

  return {
    receive,
    disconnect: (socket) => {
      leave(socket, "closed")
    },
    wake,
    isIdle: () => peerSockets().length === 0 && pending.length === 0,
  }
}
