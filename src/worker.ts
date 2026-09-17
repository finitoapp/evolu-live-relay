import { createRelayConsole } from "./log.ts"
import {
  createRelay,
  initialSocketState,
  type Relay,
  type RelayHost,
  type RelaySocket,
  type SocketState,
} from "./relay.ts"
import { parseRoomId } from "./room.ts"

/**
 * evolu-live-relay as a Cloudflare Durable Object: the host adapter. The relay
 * itself is in relay.ts, shared with the local server. See DESIGN.md §4b.
 *
 * Connect to `wss://<host>/<roomId>`: the Worker routes the path to one
 * Durable Object per room, which is where that owner's devices meet — and one
 * object is one relay. The room id is opaque here; what it is derived from is
 * the app's business (DESIGN.md §4b), and the owner is never in the address —
 * the relay reads it out of the rounds themselves. The object writes no
 * rows; the only storage API it touches is the alarm clock, which is how a
 * held round gets routed once a pipe goes quiet. Everything else lives in the
 * per-socket attachments, so it may hibernate between messages.
 *
 * Deploy: bunx wrangler deploy
 */

/** The Workers types this file needs; the repo has no @cloudflare/workers-types. */
interface CfWebSocket extends Omit<WebSocket, "send"> {
  readonly send: (data: Uint8Array | string) => void
  readonly serializeAttachment: (value: unknown) => void
  readonly deserializeAttachment: () => unknown
}

interface DurableObjectState {
  readonly id: { readonly name?: string }
  readonly acceptWebSocket: (socket: CfWebSocket) => void
  readonly getWebSockets: () => Array<CfWebSocket>
  /** The alarm clock is the only storage API here; no rows are ever written. */
  readonly storage: {
    readonly setAlarm: (scheduledTime: number) => Promise<void>
  }
}

interface DurableObjectNamespace {
  readonly idFromName: (name: string) => unknown
  readonly get: (id: unknown) => {
    readonly fetch: (request: Request) => Promise<Response>
  }
}

interface Env {
  readonly RELAY: DurableObjectNamespace
}

declare const WebSocketPair: new () => {
  readonly 0: CfWebSocket
  readonly 1: CfWebSocket
}

/**
 * The attachment is the socket's state, and the only thing that survives
 * hibernation — so `state`/`setState` are literally the attachment codec.
 */
const socketView = (socket: CfWebSocket): RelaySocket => ({
  send: (bytes) => {
    socket.send(bytes)
  },
  state: () => socket.deserializeAttachment() as SocketState,
  setState: (next) => {
    socket.serializeAttachment(next)
  },
})

/** The composition root, one per isolate. */
const console = createRelayConsole()

const upgradeRequired = () =>
  new Response("evolu-live-relay — WebSocket only", { status: 426 })

export class EvoluLiveRelay {
  readonly #ctx: DurableObjectState
  readonly #host: RelayHost
  /** Built on first use, from the owner a connecting socket brought. */
  #relay: Relay | null = null
  #tiebreak = 0

  constructor(ctx: DurableObjectState) {
    this.#ctx = ctx
    this.#host = {
      openSockets: () =>
        this.#ctx
          .getWebSockets()
          .filter((socket) => socket.readyState === 1)
          .map(socketView),
      // Hibernation makes an alarm the only way back, so there is nothing to
      // cancel: a wake with nothing due just rearms and goes back to sleep.
      wakeAt: (delayMs) => {
        if (delayMs === null) return
        // The relay is synchronous, so this is fire-and-forget — a lost alarm
        // only delays a held round — but the rejection has to go somewhere.
        this.#ctx.storage
          .setAlarm(Date.now() + delayMs)
          .catch((error: unknown) => {
            console.error("alarm-failed", { reason: String(error) })
          })
      },
      now: () => Date.now(),
      console,
    }
  }

  /** Null when this object was not addressed by a valid room id. */
  #relayFor(): Relay | null {
    if (this.#relay !== null) return this.#relay

    const name = this.#ctx.id.name
    if (name === undefined) return null

    const roomId = parseRoomId(name)
    if (!roomId.ok) return null

    this.#relay = createRelay(roomId.value, this.#host)
    return this.#relay
  }

  fetch(request: Request): Response {
    if (request.headers.get("upgrade") !== "websocket") {
      return upgradeRequired()
    }

    const pair = new WebSocketPair()
    this.#ctx.acceptWebSocket(pair[1])
    socketView(pair[1]).setState(initialSocketState(this.#nextId()))

    // `webSocket` is a Workers-only ResponseInit field.
    return new Response(null, {
      status: 101,
      webSocket: pair[0],
    } as unknown as ResponseInit)
  }

  webSocketMessage(socket: CfWebSocket, message: string | ArrayBuffer): void {
    if (typeof message === "string") return
    this.#relayFor()?.receive(socketView(socket), new Uint8Array(message))
  }

  webSocketClose(socket: CfWebSocket): void {
    this.#relayFor()?.disconnect(socketView(socket))
  }

  alarm(): void {
    this.#relayFor()?.wake()
  }

  /** Monotonic across hibernation, because it is time based. */
  #nextId(): number {
    this.#tiebreak = (this.#tiebreak + 1) % 1000
    return Date.now() * 1000 + this.#tiebreak
  }
}

export default {
  fetch: (request: Request, env: Env): Response | Promise<Response> => {
    const url = new URL(request.url)
    const roomId = url.pathname.replace(/^\/+|\/+$/g, "")

    // The room id is routing and nothing else; what it is derived from is the
    // app's business, so only its shape is checked.
    const room = parseRoomId(roomId)
    if (!room.ok) {
      // A browser shows none of this — its WebSocket API exposes neither the
      // status nor the body — so say it in the log as well, where it can be
      // read with `wrangler tail`.
      console.warn("refused", { reason: room.error.reason })
      // The host comes from the request, so the line can be pasted as it is.
      return new Response(
        `${room.error.reason}\n\nConnect to wss://${url.host}/<roomId>\n`,
        { status: 400 }
      )
    }

    if (request.headers.get("upgrade") !== "websocket") {
      return upgradeRequired()
    }

    return env.RELAY.get(env.RELAY.idFromName(roomId)).fetch(request)
  },
}
