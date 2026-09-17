import {
  type AppOwner,
  createAppOwner,
  createConsole,
  createOwnerSecret,
  createRandomBytes,
  Millis,
} from "@evolu/common"
import {
  createProtocolMessageBuffer,
  createTimestamp,
  createTimestampsBuffer,
  type EncryptedDbChange,
  InfiniteUpperBound,
  MessageType,
  ProtocolErrorCode,
  type ProtocolMessage,
  RangeType,
  type SubscriptionFlag,
  SubscriptionFlags,
} from "@evolu/common/local-first"
import { expect, test } from "vitest"
import {
  createRelay,
  initialSocketState,
  type RelaySocket,
  type SocketState,
} from "./relay.ts"
import { answerTimeoutMs, pipeIdleMs } from "./routing.ts"

/**
 * The relay against a fake host: no sockets, no server, and a clock that is
 * moved by hand, so a sweep that takes seconds in server.test.ts takes none
 * here. What this covers is the state machine — who becomes a peer, who is
 * handed a round, what a timeout does — and it covers it for both hosts at
 * once, because both run this exact code.
 */

const messageTypeIndex = 1 + 16
const errorCodeIndex = messageTypeIndex + 1
const requestBodyOffset = messageTypeIndex + 1 + 1 + 16 + 1
const responseBodyOffset = messageTypeIndex + 2

const createOwner = () =>
  createAppOwner(createOwnerSecret({ randomBytes: createRandomBytes() }))

const createRequest = (
  owner: AppOwner,
  {
    tag = 1,
    writeKey = owner.writeKey,
    ranges = true,
    subscriptionFlag = SubscriptionFlags.Subscribe,
  }: {
    tag?: number
    writeKey?: typeof owner.writeKey
    ranges?: boolean
    subscriptionFlag?: SubscriptionFlag
  } = {}
): ProtocolMessage => {
  const buffer = createProtocolMessageBuffer(owner.id, {
    messageType: MessageType.Request,
    writeKey,
    subscriptionFlag,
  })
  // One message, so the round carries data the way a real sync round does.
  // `tag` only makes one round's bytes tell apart from another's.
  buffer.addMessage({
    timestamp: createTimestamp({ millis: Millis.orThrow(1) }),
    change: new Uint8Array([tag, 2, 3]) as EncryptedDbChange,
  })
  // No range means the sender wants no further round.
  if (ranges) {
    buffer.addRange({
      type: RangeType.Timestamps,
      upperBound: InfiniteUpperBound,
      timestamps: createTimestampsBuffer(),
    })
  }
  return buffer.unwrap()
}

interface TestSocket extends RelaySocket {
  readonly sent: Array<Uint8Array>
  open: boolean
}

const createHarness = (owner: AppOwner) => {
  let now = 1_000_000
  let wakeAt: number | null = null
  let nextId = 1
  const sockets: Array<TestSocket> = []

  const relay = createRelay(owner.id, {
    openSockets: () => sockets.filter((socket) => socket.open),
    wakeAt: (delayMs) => {
      wakeAt = delayMs === null ? null : now + delayMs
    },
    now: () => now,
    // Silent, because these tests are about what the relay routes, not what it
    // says about it; raise the level to watch a case unfold.
    console: createConsole({ level: "silent" }),
  })

  const connect = (): TestSocket => {
    let state = initialSocketState(nextId++)
    const socket: TestSocket = {
      sent: [],
      open: true,
      send: (bytes) => {
        socket.sent.push(bytes)
      },
      state: () => state,
      setState: (next: SocketState) => {
        state = next
      },
    }
    sockets.push(socket)
    return socket
  }

  return {
    relay,
    connect,
    send: (socket: TestSocket, message: ProtocolMessage) => {
      relay.receive(socket, message)
    },
    close: (socket: TestSocket) => {
      socket.open = false
      relay.disconnect(socket)
    },
    /** Moves the clock, firing every wake the relay asked for on the way. */
    advance: (ms: number) => {
      const target = now + ms
      for (let fired = 0; fired < 100; fired++) {
        if (wakeAt === null || wakeAt > target) break
        now = wakeAt
        wakeAt = null
        relay.wake()
      }
      now = target
      if (wakeAt !== null && wakeAt <= now) {
        wakeAt = null
        relay.wake()
      }
    },
    isIdle: () => relay.isIdle(),
  }
}

/** The header-only "you are in sync" answer. */
const expectInSync = (message: Uint8Array | undefined) => {
  expect(message).toBeDefined()
  expect(message?.[messageTypeIndex]).toBe(MessageType.Response)
  expect(message?.[errorCodeIndex]).toBe(ProtocolErrorCode.NoError)
  expect(message?.length).toBe(responseBodyOffset + 1)
}

test("a lone device is told it is in sync", () => {
  const owner = createOwner()
  const relay = createHarness(owner)
  const alone = relay.connect()

  relay.send(alone, createRequest(owner))

  expect(alone.sent).toHaveLength(1)
  expectInSync(alone.sent[0])
})

test("a peer is piped the round as a Response, body untouched", () => {
  const owner = createOwner()
  const relay = createHarness(owner)
  const a = relay.connect()
  const b = relay.connect()

  relay.send(a, createRequest(owner))
  const request = createRequest(owner, { tag: 2 })
  relay.send(b, request)

  const piped = a.sent[1]
  expect(piped?.[messageTypeIndex]).toBe(MessageType.Response)
  expect(piped?.[errorCodeIndex]).toBe(ProtocolErrorCode.NoError)
  // Version and owner id verbatim, body untouched.
  expect(piped?.subarray(0, messageTypeIndex)).toEqual(
    request.subarray(0, messageTypeIndex)
  )
  expect(piped?.subarray(responseBodyOffset)).toEqual(
    request.subarray(requestBodyOffset)
  )
})

test("a socket that has connected but never synced is not a peer", () => {
  const owner = createOwner()
  const relay = createHarness(owner)
  const a = relay.connect()
  // Connected, and silent: it has not introduced itself, so it cannot be
  // handed another device's round.
  const quiet = relay.connect()

  relay.send(a, createRequest(owner))

  expectInSync(a.sent[0])
  expect(quiet.sent).toHaveLength(0)
})

test("a refused write key never becomes a peer", () => {
  const owner = createOwner()
  const relay = createHarness(owner)
  const a = relay.connect()
  const impostor = relay.connect()

  relay.send(a, createRequest(owner))
  relay.send(
    impostor,
    createRequest(owner, {
      writeKey: new Uint8Array(16).fill(9) as typeof owner.writeKey,
    })
  )

  expect(impostor.sent[0]?.[errorCodeIndex]).toBe(
    ProtocolErrorCode.WriteKeyError
  )
  expect(impostor.state().joined).toBe(false)

  // So the next round from A finds nobody to sync with.
  relay.send(a, createRequest(owner, { tag: 2 }))
  expectInSync(a.sent[1])
})

test("a round for another owner is dropped", () => {
  const owner = createOwner()
  const stranger = createOwner()
  const relay = createHarness(owner)
  const socket = relay.connect()

  relay.send(socket, createRequest(stranger))

  expect(socket.sent).toHaveLength(0)
  expect(socket.state().joined).toBe(false)
})

test("a round with no ranges ends the pipe and is broadcast to the rest", () => {
  const owner = createOwner()
  const relay = createHarness(owner)
  const a = relay.connect()
  const b = relay.connect()

  relay.send(a, createRequest(owner))
  relay.send(b, createRequest(owner, { tag: 2 }))
  expect(a.state().partnerId).toBe(b.state().id)

  // The last word of a conversation: nothing to pipe, and the pipe is over.
  const last = createRequest(owner, { tag: 3, ranges: false })
  relay.send(b, last)

  const broadcast = a.sent[2]
  expect(broadcast?.[messageTypeIndex]).toBe(MessageType.Broadcast)
  expect(a.state().partnerId).toBeNull()
  expect(b.state().partnerId).toBeNull()
})

test("an unsubscribing socket stops being a peer, and may rejoin", () => {
  const owner = createOwner()
  const relay = createHarness(owner)
  const a = relay.connect()
  const b = relay.connect()

  relay.send(a, createRequest(owner))
  relay.send(
    b,
    createRequest(owner, {
      subscriptionFlag: SubscriptionFlags.Unsubscribe,
    })
  )
  expect(b.state().left).toBe(true)

  // A is alone again.
  relay.send(a, createRequest(owner, { tag: 2 }))
  expectInSync(a.sent[1])

  // And B's next round is an introduction, so it is piped A's data.
  relay.send(b, createRequest(owner, { tag: 3 }))
  expect(b.state().joined).toBe(true)
})

test("a silent partner is retried, then the sender is told it is in sync", () => {
  const owner = createOwner()
  const relay = createHarness(owner)
  const a = relay.connect()
  const b = relay.connect()

  relay.send(a, createRequest(owner))
  relay.send(b, createRequest(owner, { tag: 2 }))
  expect(a.sent).toHaveLength(2)

  // A never answers, so B's round runs out of time and has nobody left to try.
  relay.advance(answerTimeoutMs + 1)

  expect(a.sent).toHaveLength(2)
  expectInSync(b.sent[0])
  expect(a.state().partnerId).toBeNull()
})

test("a held round sweeps the peers one at a time", () => {
  const owner = createOwner()
  const relay = createHarness(owner)
  const a = relay.connect()
  const b = relay.connect()

  relay.send(a, createRequest(owner))
  relay.send(b, createRequest(owner, { tag: 2 }))

  // Let both the answer and the pipe run out, so A and B are free peers again.
  relay.advance(pipeIdleMs + 1)

  const c = relay.connect()
  const request = createRequest(owner, { tag: 3 })
  const body = request.subarray(requestBodyOffset)
  relay.send(c, request)

  // Among free peers the newest wins, so B is asked first.
  expect(b.sent.at(-1)?.subarray(responseBodyOffset)).toEqual(body)

  relay.advance(answerTimeoutMs + 1)
  expect(a.sent.at(-1)?.subarray(responseBodyOffset)).toEqual(body)

  // Nobody left to ask.
  relay.advance(answerTimeoutMs + 1)
  expectInSync(c.sent.at(-1))
})

test("a closing socket takes its held rounds and its partner's pipe with it", () => {
  const owner = createOwner()
  const relay = createHarness(owner)
  const a = relay.connect()
  const b = relay.connect()

  relay.send(a, createRequest(owner))
  relay.send(b, createRequest(owner, { tag: 2 }))
  expect(a.state().partnerId).toBe(b.state().id)

  relay.close(b)

  // A is free again, not left pointing at a socket that is gone.
  expect(a.state().partnerId).toBeNull()

  // And B's round is not swept on to A once the answer runs out.
  const before = a.sent.length
  relay.advance(answerTimeoutMs + pipeIdleMs)
  expect(a.sent).toHaveLength(before)
})

test("the relay goes idle once everyone has left", () => {
  const owner = createOwner()
  const relay = createHarness(owner)
  const a = relay.connect()
  const b = relay.connect()

  relay.send(a, createRequest(owner))
  relay.send(b, createRequest(owner, { tag: 2 }))
  expect(relay.isIdle()).toBe(false)

  relay.close(a)
  relay.close(b)
  expect(relay.isIdle()).toBe(true)
})
