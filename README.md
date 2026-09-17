# evolu-live-relay

A relay for [Evolu](https://www.evolu.dev) that **stores nothing**. It pairs the
devices of one owner while they are online together and pipes their sync rounds
at each other, so they reconcile end to end. No database, no message log, no
history — when the last socket closes, nothing is left.

Hence *live*: **if two devices are not online at the same time, no data moves.**
A device that was offline while another one wrote is not caught up by this. That
is the trade for a relay you can run on a free Cloudflare plan and forget about.

A stock Evolu client talks to it unchanged.

## How it works

Request, Response and Broadcast carry the same `[messages][ranges]` body and
differ only in a few header bytes, so a round from one device can be re-headered
and handed to another, which answers it as if it were the relay. Both sides run
Evolu's own reconciliation; the relay only decides who talks to whom and passes
bytes. It reads two counts out of each round to know whether it carries data and
whether the sender wants another round, then forgets them.

[DESIGN.md](./DESIGN.md) has the whole thing: the protocol facts it rests on,
the routing rules, and — importantly — what it deliberately does not do.

## Run it locally

```sh
bun install
bun run src/server.ts --port 4000
```

Browsers only allow `wss://` from an https page, so you need a certificate for
anything but a quick test:

```sh
mkcert -install
mkcert -key-file key.pem -cert-file cert.pem localhost 127.0.0.1 ::1
bun run src/server.ts --port 4000 --cert cert.pem --key key.pem
```

`--cert` on its own also takes a single PEM holding both key and certificate.

> The certificate has to be **trusted**, not click-through accepted. Evolu opens
> its sync sockets inside a SharedWorker, which has no tab behind it, so a
> browser cannot ask you there — it just fails, silently, forever. `mkcert`
> installs a local CA, which is why it is the short way out. A phone needs that
> CA installed too, or has to reach the relay over a port forward
> (`adb reverse tcp:4000 tcp:4000`).

## Point a client at it

A room id goes in the path and the owner id in the query string, so one
Durable Object — or one routing table entry — belongs to exactly one owner:

```
wss://<host>/<roomId>?ownerId=<ownerId>
```

`<ownerId>` is `evolu.appOwner.id` (or the id of whichever owner you sync), and
`?ownerId=…` is exactly what Evolu's `createOwnerWebSocketTransport` appends.
`<roomId>` is any Evolu `Id` — the relay treats it as opaque — but it should be
derived from the owner's secret, so that only that owner's devices can find the
room:

```ts
const secret = mnemonicToOwnerSecret(evolu.appOwner.mnemonic)
const roomId = idBytesToId(
  IdBytes.orThrow(
    createSlip21(secret, ["evolu-live-relay", "RoomId"]).slice(0, 16)
  )
)

createOwnerWebSocketTransport({
  url: `wss://<host>/${roomId}`,
  ownerId: evolu.appOwner.id,
})
```

That is the recipe Evolu uses for the owner id, under a different SLIP-21
label. Knowing an owner id therefore does not reveal its room. See DESIGN.md §6
for what that does and does not buy — it is an unguessable address, not
authentication.

## Deploy to Cloudflare

One Durable Object per room is where that owner's devices meet — the one
primitive that gives two live sockets the same single-threaded place to talk.

```sh
bunx wrangler login
bun run deploy
```

The class is registered as SQLite-backed because those are the ones the free
plan offers; it never writes a row. The only storage API it touches is the alarm
clock. Add a custom domain by putting it in `wrangler.toml`:

```toml
routes = [{ pattern = "relay.example.com", custom_domain = true }]
```

## Limits worth knowing before you use it

- **No catch-up.** Both devices must be online together. This is the design, not
  a bug.
- **Anyone who knows an owner id can connect**; the first write key seen for an
  owner wins while it has connections. Payloads stay end-to-end encrypted, and a
  client quarantines what it cannot decrypt, but there is no real auth.
- **No rate limiting, no backpressure, no quotas.**
- It rests on Evolu internals — clients processing unsolicited messages, and a
  copied reader for the wire format. Both fail safe, and both can drift on an
  Evolu upgrade. See DESIGN.md §6 for the full list.

## Development

```sh
bun run check       # biome + tsc + vitest
bun run test        # vitest
bun run dev:worker  # wrangler dev
```

The relay is written once and hosted twice. `src/wire.ts` is the byte layer and
`src/routing.ts` the pure routing decision; `src/relay.ts` is the relay itself,
the state machine built on those two; and `src/server.ts` (Bun) and
`src/worker.ts` (Cloudflare Durable Object) are thin adapters that give it
sockets, a clock and a place to keep per-socket state.

The tests follow the same lines: `routing.test.ts` for the routing decision,
`wire.test.ts` for the wire-format reader against Evolu's own encoder — that
one is the alarm for the day the format moves — `relay.test.ts` for the state
machine against a fake host and a hand-moved clock, and `server.test.ts` for
the Bun adapter over a real WebSocket.

## License

MIT
