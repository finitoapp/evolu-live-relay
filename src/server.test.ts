import { spawn } from "node:child_process"
import {
  type AppOwner,
  createAppOwner,
  createOwnerSecret,
  createRandomBytes,
  createSlip21,
  IdBytes,
  idBytesToId,
  Millis,
  mnemonicToOwnerSecret,
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
  SubscriptionFlags,
} from "@evolu/common/local-first"
import { afterAll, beforeAll, expect, test } from "vitest"

/**
 * Drives src/server.ts over a real WebSocket with real protocol messages.
 * It checks the routing and the header rewriting, which is all the relay does;
 * proving that two live Evolu clients converge through it is a manual step,
 * see DESIGN.md §5.
 */

const messageTypeIndex = 1 + 16
const errorCodeIndex = messageTypeIndex + 1
const requestBodyOffset = messageTypeIndex + 1 + 1 + 16 + 1
const responseBodyOffset = messageTypeIndex + 2
const broadcastBodyOffset = messageTypeIndex + 1

/** A fresh owner per test, so tests never share relay state. */
const createOwner = () =>
  createAppOwner(createOwnerSecret({ randomBytes: createRandomBytes() }))

/**
 * The room the owner's devices meet in — the derivation DESIGN.md §4b
 * recommends, done the way an app would: a SLIP-21 sibling of the secret the
 * mnemonic carries, so every device reaches it and nobody else can guess it.
 */
const roomIdFor = (owner: AppOwner) =>
  idBytesToId(
    IdBytes.orThrow(
      createSlip21(mnemonicToOwnerSecret(owner.mnemonic), [
        "evolu-live-relay",
        "RoomId",
      ]).slice(0, 16)
    )
  )

const createRequest = (
  owner: AppOwner,
  {
    tag = 1,
    writeKey = owner.writeKey,
  }: { tag?: number; writeKey?: typeof owner.writeKey } = {}
): ProtocolMessage => {
  const buffer = createProtocolMessageBuffer(owner.id, {
    messageType: MessageType.Request,
    writeKey,
    subscriptionFlag: SubscriptionFlags.Subscribe,
  })
  // One message, so the round carries data the way a real sync round does.
  // `tag` only makes one round's bytes tell apart from another's.
  buffer.addMessage({
    timestamp: createTimestamp({ millis: Millis.orThrow(1) }),
    change: new Uint8Array([tag, 2, 3]) as EncryptedDbChange,
  })
  buffer.addRange({
    type: RangeType.Timestamps,
    upperBound: InfiniteUpperBound,
    timestamps: createTimestampsBuffer(),
  })
  return buffer.unwrap()
}

/** Waits for a piped round carrying exactly this body. */
const pipedRoundWithBody = async (client: Client, body: Uint8Array) => {
  for (let attempt = 0; attempt < 10; attempt++) {
    const message = await nextPipedRound(client)
    const received = message.subarray(responseBodyOffset)
    if (
      received.length === body.length &&
      received.every((byte, index) => byte === body[index])
    ) {
      return true
    }
  }
  throw new Error("The round never arrived")
}

/** The header-only "you are in sync" answer, whoever it is meant for. */
const nextInSync = async (client: Client, timeoutMs = 6_000) => {
  for (let attempt = 0; attempt < 8; attempt++) {
    const message = await client.next(timeoutMs)
    if (
      message[messageTypeIndex] === MessageType.Response &&
      message.length === responseBodyOffset + 1
    ) {
      return message
    }
  }
  throw new Error("No in-sync answer arrived")
}

/**
 * Skips broadcasts and the header-only "you are in sync" answers — these test
 * sockets never reply, so every round they are handed ends in one — until a
 * response carrying a piped round arrives.
 */
const nextPipedRound = async (client: Client, timeoutMs = 6_000) => {
  for (let attempt = 0; attempt < 8; attempt++) {
    const message = await client.next(timeoutMs)
    if (
      message[messageTypeIndex] === MessageType.Response &&
      message.length > responseBodyOffset + 1
    ) {
      return message
    }
  }
  throw new Error("No piped round arrived")
}

/** A fresh copy, because send() wants a plain ArrayBuffer-backed view. */
const send = (client: Client, message: ProtocolMessage) => {
  client.socket.send(new Uint8Array(message))
}

interface Client {
  readonly socket: WebSocket
  readonly received: Array<Uint8Array>
  readonly next: (timeoutMs?: number) => Promise<Uint8Array>
}

let relay: ReturnType<typeof spawn>
let port = 0
const clients: Array<Client> = []

const connect = async (owner: AppOwner): Promise<Client> => {
  const socket = new WebSocket(`ws://localhost:${port}/${roomIdFor(owner)}`)
  socket.binaryType = "arraybuffer"
  const received: Array<Uint8Array> = []
  let notify: (() => void) | null = null

  socket.addEventListener("message", (event: MessageEvent) => {
    received.push(new Uint8Array(event.data as ArrayBuffer))
    notify?.()
  })

  await new Promise<void>((resolve, reject) => {
    socket.addEventListener("open", () => {
      resolve()
    })
    socket.addEventListener("error", () => {
      reject(new Error("WebSocket failed to open"))
    })
  })

  let read = 0
  const client: Client = {
    socket,
    received,
    next: (timeoutMs = 2_000) =>
      new Promise<Uint8Array>((resolve, reject) => {
        const take = () => {
          const message = received[read]
          if (message === undefined) return false
          read += 1
          resolve(message)
          return true
        }
        if (take()) return
        const timeout = setTimeout(() => {
          notify = null
          reject(new Error(`No message arrived within ${timeoutMs}ms`))
        }, timeoutMs)
        notify = () => {
          if (take()) {
            clearTimeout(timeout)
            notify = null
          }
        }
      }),
  }

  clients.push(client)
  return client
}

beforeAll(async () => {
  relay = spawn("bun", ["run", "src/server.ts", "--port", "0"], {
    stdio: ["ignore", "pipe", "inherit"],
  })

  port = await new Promise<number>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error("The relay did not report a port within 10s"))
    }, 10_000)

    relay.stdout?.on("data", (chunk: Buffer) => {
      const match = /wss?:\/\/localhost:(\d+)/.exec(chunk.toString())
      if (match?.[1] === undefined) return
      clearTimeout(timeout)
      resolve(Number(match[1]))
    })
  })
})

afterAll(() => {
  for (const client of clients) client.socket.close()
  relay.kill()
})

test("a lone device is told it is in sync", async () => {
  const owner = createOwner()
  const alone = await connect(owner)
  send(alone, createRequest(owner))

  const response = await alone.next()
  expect(response[messageTypeIndex]).toBe(MessageType.Response)
  expect(response[errorCodeIndex]).toBe(ProtocolErrorCode.NoError)
  // No ranges and no messages beyond the "zero messages" byte, so the client
  // has nothing to answer.
  expect(response.length).toBe(responseBodyOffset + 1)

  alone.socket.close()
})

test("a peer is piped the request as a response, body untouched", async () => {
  const owner = createOwner()
  const a = await connect(owner)
  send(a, createRequest(owner))
  await a.next()

  // A socket announces its owner by syncing, the way a client does on connect.
  const b = await connect(owner)
  const request = createRequest(owner)
  send(b, request)

  const piped = await a.next()
  expect(piped[messageTypeIndex]).toBe(MessageType.Response)
  expect(piped[errorCodeIndex]).toBe(ProtocolErrorCode.NoError)
  // Version and owner id verbatim, body untouched.
  expect(piped.subarray(0, messageTypeIndex)).toEqual(
    request.subarray(0, messageTypeIndex)
  )
  expect(piped.subarray(responseBodyOffset)).toEqual(
    request.subarray(requestBodyOffset)
  )

  a.socket.close()
  b.socket.close()
})

test("a third device gets a broadcast, then the pipe frees up", async () => {
  const owner = createOwner()
  const a = await connect(owner)
  send(a, createRequest(owner))
  await a.next()

  const b = await connect(owner)
  send(b, createRequest(owner))
  await a.next()

  // A and B are now piped together, so C's round is broadcast to both and
  // held until the pipe goes idle.
  const c = await connect(owner)
  const request = createRequest(owner)
  send(c, request)

  const broadcast = await a.next()
  expect(broadcast[messageTypeIndex]).toBe(MessageType.Broadcast)
  expect(broadcast.subarray(broadcastBodyOffset)).toEqual(
    request.subarray(requestBodyOffset)
  )

  // Once the pipe is stale the held round is piped to the newest free peer.
  const piped = await nextPipedRound(b)
  expect(piped[messageTypeIndex]).toBe(MessageType.Response)
  expect(piped.subarray(responseBodyOffset)).toEqual(
    request.subarray(requestBodyOffset)
  )

  a.socket.close()
  b.socket.close()
  c.socket.close()
}, 10_000)

test("a second write key for the same owner is rejected", async () => {
  const owner = createOwner()
  const first = await connect(owner)
  send(first, createRequest(owner))
  await first.next()

  const impostor = await connect(owner)
  send(
    impostor,
    createRequest(owner, {
      writeKey: new Uint8Array(16).fill(9) as typeof owner.writeKey,
    })
  )

  const rejection = await impostor.next()
  expect(rejection[messageTypeIndex]).toBe(MessageType.Response)
  expect(rejection[errorCodeIndex]).toBe(ProtocolErrorCode.WriteKeyError)

  first.socket.close()
  impostor.socket.close()
})

test("a peer that went quiet does not swallow another device's round", async () => {
  // A device whose network dropped: the socket is still open here, it just
  // never answers again. It must not be handed a fresh device's sync round.
  const owner = createOwner()
  const zombie = await connect(owner)
  send(zombie, createRequest(owner))
  await zombie.next()

  const b = await connect(owner)
  send(b, createRequest(owner))

  const a = await connect(owner)
  const request = createRequest(owner)
  send(a, request)

  // B must get A's round as a Response it can answer, not only as a Broadcast.
  const piped = await nextPipedRound(b)
  expect(piped[messageTypeIndex]).toBe(MessageType.Response)
  expect(piped.subarray(responseBodyOffset)).toEqual(
    request.subarray(requestBodyOffset)
  )

  zombie.socket.close()
  a.socket.close()
  b.socket.close()
}, 20_000)

test("a silent partner is retried, then the sender is told it is in sync", async () => {
  const owner = createOwner()

  // Two devices that join and then never answer anything again.
  const first = await connect(owner)
  send(first, createRequest(owner, { tag: 1 }))
  const second = await connect(owner)
  send(second, createRequest(owner, { tag: 2 }))

  const sender = await connect(owner)
  const request = createRequest(owner, { tag: 3 })
  send(sender, request)
  const body = request.subarray(requestBodyOffset)

  // The round sweeps both peers, one answer timeout apart.
  expect(await pipedRoundWithBody(second, body)).toBe(true)
  expect(await pipedRoundWithBody(first, body)).toBe(true)

  // Nobody left to ask, so the sender is answered the way a lone device is.
  const answer = await nextInSync(sender)
  expect(answer[errorCodeIndex]).toBe(ProtocolErrorCode.NoError)

  first.socket.close()
  second.socket.close()
  sender.socket.close()
}, 20_000)

test("an address without a room is refused", async () => {
  const response = await fetch(`http://localhost:${port}/`)

  expect(response.status).toBe(400)
  expect(await response.text()).toContain("/<roomId>")
})

test("a second owner's rounds are dropped once a room has settled", async () => {
  const owner = createOwner()
  const stranger = createOwner()

  // The first round settles whose room it is.
  const device = await connect(owner)
  send(device, createRequest(owner))
  await device.next()

  // Same room, another owner: nothing comes back, and it never becomes a peer,
  // so the device that belongs here is still answered as if it were alone.
  const impostor = await connect(owner)
  send(impostor, createRequest(stranger))

  send(device, createRequest(owner, { tag: 2 }))
  const answer = await nextInSync(device)
  expect(answer[errorCodeIndex]).toBe(ProtocolErrorCode.NoError)

  device.socket.close()
  impostor.socket.close()
})
