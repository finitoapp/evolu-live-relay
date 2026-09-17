import { err, Id, ok, type Result, tryAsync } from "@evolu/common"
import { defineError } from "./error.ts"
import { createRelayConsole } from "./log.ts"
import {
  createRelay,
  initialSocketState,
  type Relay,
  type RelaySocket,
  type SocketState,
} from "./relay.ts"

/**
 * evolu-live-relay, the local server: the host adapter. The relay itself is in
 * relay.ts, shared with the Cloudflare port in worker.ts. See DESIGN.md §4.
 *
 * It speaks enough of the Evolu protocol that a stock client connects to it and
 * syncs, but it stores nothing: no database, no message, no history. It pairs
 * two connected devices of the same owner and pipes their protocol messages at
 * each other, so the reconciliation happens end to end between the two clients;
 * every other socket of that owner gets a Broadcast copy. A device connected
 * alone is answered "you are in sync" and its data is dropped.
 *
 * Connect to `ws://<host>/<roomId>`, the same address the Worker takes. One
 * process here holds every room, so this adapter keeps a relay per room where
 * a Durable Object is one; the room id is opaque to it either way, and the
 * owner is never in the address — the relay reads it out of the rounds.
 *
 * Run: bun run src/server.ts [--port 4000] [--cert file.pem [--key file.pem]]
 *
 * Browsers only allow wss:// from an https page, so pass a certificate for
 * anything but a quick local test. `--cert` alone takes a PEM holding both the
 * key and the certificate. The certificate has to be **trusted**, not
 * click-through accepted: sync sockets are opened from a SharedWorker, where a
 * browser cannot ask. `mkcert` is the short way there.
 */

interface SocketData {
  readonly id: number
  readonly roomId: Id
  /** The relay owns the contents; this is only where they are kept. */
  state: SocketState
}

/** The subset of Bun's WebSocket this script uses; the repo has no bun-types. */
interface Socket {
  readonly data: SocketData
  readonly readyState: number
  readonly send: (data: Uint8Array) => number
}

declare const Bun: {
  readonly file: (path: string) => { readonly text: () => Promise<string> }
  readonly serve: (options: {
    readonly port: number
    readonly tls?: { readonly cert: string; readonly key: string }
    readonly fetch: (
      request: Request,
      server: {
        readonly upgrade: (
          request: Request,
          options: { readonly data: SocketData }
        ) => boolean
      }
    ) => Response | undefined
    readonly websocket: {
      readonly idleTimeout?: number
      readonly sendPings?: boolean
      readonly open: (socket: Socket) => void
      readonly message: (socket: Socket, message: string | Uint8Array) => void
      readonly close: (socket: Socket) => void
    }
  }) => { readonly port: number }
}

interface HostedRelay {
  readonly relay: Relay
  /** Sockets of this room, open or not; the relay filters. */
  readonly sockets: Array<Socket>
  readonly cancelWake: () => void
}

/**
 * The composition root: every dependency the relay is given starts here, and
 * nothing below reaches for a global.
 */
const console = createRelayConsole()

const isOpen = (socket: Socket) => socket.readyState === 1

const relays = new Map<Id, HostedRelay>()

/** One socket as its room's relay sees it. */
const socketView = (socket: Socket): RelaySocket => ({
  send: (bytes) => {
    socket.send(bytes)
  },
  state: () => socket.data.state,
  setState: (next) => {
    socket.data.state = next
  },
})

const hostedRelayFor = (roomId: Id): HostedRelay => {
  const existing = relays.get(roomId)
  if (existing !== undefined) return existing

  const sockets: Array<Socket> = []
  let timer: ReturnType<typeof setTimeout> | null = null

  const cancelWake = () => {
    if (timer !== null) clearTimeout(timer)
    timer = null
  }

  const relay = createRelay(roomId, {
    openSockets: () => sockets.filter(isOpen).map(socketView),
    wakeAt: (delayMs) => {
      cancelWake()
      if (delayMs === null) return
      timer = setTimeout(() => {
        timer = null
        relay.wake()
      }, delayMs)
    },
    now: () => Date.now(),
    console,
  })

  const created: HostedRelay = { relay, sockets, cancelWake }
  relays.set(roomId, created)
  return created
}

/**
 * Nothing connected and nothing held: the room is forgotten, which is also
 * what ends the life of its write key.
 */
const dropIfIdle = (roomId: Id) => {
  const hosted = relays.get(roomId)
  if (hosted === undefined || !hosted.relay.isIdle()) return

  hosted.cancelWake()
  relays.delete(roomId)
}

const handleMessage = (socket: Socket, message: Uint8Array) => {
  const { roomId } = socket.data
  relays.get(roomId)?.relay.receive(socketView(socket), message)
  dropIfIdle(roomId)
}

const handleClose = (socket: Socket) => {
  const { roomId } = socket.data
  const hosted = relays.get(roomId)

  if (hosted !== undefined) {
    hosted.relay.disconnect(socketView(socket))

    const index = hosted.sockets.indexOf(socket)
    if (index !== -1) hosted.sockets.splice(index, 1)

    dropIfIdle(roomId)
  }

  console.info("close", { socket: socket.data.id })
}

const createArgumentsError = defineError("ArgumentsError")<{
  readonly reason: string
}>()
type ArgumentsError = ReturnType<typeof createArgumentsError>

interface Arguments {
  readonly port: number
  readonly certPath: string | undefined
  readonly keyPath: string | undefined
}

/**
 * Every flag is read and checked here rather than where it is used, so a typo
 * ends in one line instead of a stack trace out of `Bun.serve` — or, worse,
 * in silence: a bare `--cert` used to start a plaintext server, which a
 * browser then refuses from its SharedWorker without saying anything
 * (DESIGN.md §5).
 */
const parseArguments = (
  argv: ReadonlyArray<string>
): Result<Arguments, ArgumentsError> => {
  const flag = (name: string): Result<string | undefined, ArgumentsError> => {
    const index = argv.indexOf(name)
    if (index === -1) return ok(undefined)
    const value = argv[index + 1]
    return value === undefined || value.startsWith("--")
      ? err(createArgumentsError({ reason: `${name} needs a value` }))
      : ok(value)
  }

  const portFlag = flag("--port")
  if (!portFlag.ok) return portFlag
  const certFlag = flag("--cert")
  if (!certFlag.ok) return certFlag
  const keyFlag = flag("--key")
  if (!keyFlag.ok) return keyFlag

  const port = portFlag.value === undefined ? 4000 : Number(portFlag.value)
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    return err(
      createArgumentsError({
        reason: `--port must be a whole number 0-65535, not ${portFlag.value}`,
      })
    )
  }

  // One PEM may hold both, which is what `--cert` on its own means — but a key
  // with no certificate is a mistake, not a shorthand.
  if (certFlag.value === undefined && keyFlag.value !== undefined) {
    return err(createArgumentsError({ reason: "--key needs --cert" }))
  }

  return ok({ port, certPath: certFlag.value, keyPath: keyFlag.value })
}

const parsedArguments = parseArguments(process.argv)
if (!parsedArguments.ok) {
  console.error(parsedArguments.error.reason)
  process.exit(1)
}
const { port, certPath, keyPath } = parsedArguments.value

const createTlsFilesError = defineError("TlsFilesError")<{
  readonly paths: ReadonlyArray<string>
  readonly cause: unknown
}>()
type TlsFilesError = ReturnType<typeof createTlsFilesError>

/**
 * A path that cannot be read is an expected failure — a typo in a flag, not a
 * bug — so it comes back as a Result and ends in one legible line instead of a
 * stack trace from the top-level await.
 */
const readTls = (
  certPath: string,
  keyPath: string | undefined
): Promise<Result<{ cert: string; key: string }, TlsFilesError>> =>
  tryAsync(
    async () => {
      const cert = await Bun.file(certPath).text()
      const key = keyPath === undefined ? cert : await Bun.file(keyPath).text()
      return { cert, key }
    },
    (cause) =>
      createTlsFilesError({
        paths: keyPath === undefined ? [certPath] : [certPath, keyPath],
        cause,
      })
  )

const readTlsOrExit = async (
  certPath: string,
  keyPath: string | undefined
): Promise<{ cert: string; key: string }> => {
  const result = await readTls(certPath, keyPath)
  if (result.ok) return result.value

  const { paths, cause } = result.error
  console.error(
    `cannot read ${paths.join(" and ")}: ${
      cause instanceof Error ? cause.message : String(cause)
    }`
  )
  process.exit(1)
}

const tls =
  certPath === undefined ? undefined : await readTlsOrExit(certPath, keyPath)

let nextSocketId = 1

/**
 * The room is settled before the upgrade, so a socket belongs to exactly one
 * for its whole life — the same shape a Durable Object gets from being
 * addressed by room.
 */
const accept = (
  request: Request,
  upgrade: (options: { readonly data: SocketData }) => boolean
): Response | undefined => {
  const url = new URL(request.url)
  const roomId = url.pathname.replace(/^\/+|\/+$/g, "")

  if (!Id.is(roomId)) {
    return new Response(`Connect to ws://${url.host}/<roomId>`, { status: 400 })
  }

  const id = nextSocketId++
  return upgrade({ data: { id, roomId, state: initialSocketState(id) } })
    ? undefined
    : new Response("evolu-live-relay — WebSocket only", { status: 426 })
}

const server = Bun.serve({
  port,
  tls,
  fetch: (request, bun) =>
    accept(request, (options) => bun.upgrade(request, options)),
  websocket: {
    // A device whose network dropped leaves a socket that stays open here and
    // will never answer again. Pings keep a live peer's timer fresh (the
    // browser pongs without involving its JS) and reap a dead one in ~30s —
    // and the failing pong is also how that browser learns its socket is gone,
    // so it reconnects instead of sitting on a connection to nowhere.
    idleTimeout: 30,
    sendPings: true,
    open: (socket) => {
      const { roomId } = socket.data
      hostedRelayFor(roomId).sockets.push(socket)
      console.info("open", { socket: socket.data.id, room: roomId })
    },
    message: (socket, message) => {
      if (typeof message === "string") {
        console.debug("drop", { socket: socket.data.id, reason: "text-frame" })
        return
      }
      handleMessage(socket, message)
    },
    close: handleClose,
  },
})

console.info(
  `listening on ${tls === undefined ? "ws" : "wss"}://localhost:${
    server.port
  } — nothing is stored`
)
