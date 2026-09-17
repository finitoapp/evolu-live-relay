# evolu-live-relay — design

A server that speaks the Evolu relay protocol over WebSocket but **stores
nothing**. It exists only to let two or more devices of the same owner exchange
data while they are connected at the same time. With a single device connected
it answers "we are in sync" and drops whatever it is given.

It is a relay only in the sense the clients mean it — the thing a transport
points at. It is not a replacement for one: no persistence, no catch-up for a
device that was offline while another device changed something, no history.
Hence *live*: if two devices are not online together, no data moves.

Evolu is an external dependency and stays untouched — this uses only its public
exports.

## 1. Goal

- Speak enough of the Evolu protocol that a stock Evolu client connects to it
  and syncs without any client-side change.
- Route data between the connected devices of one owner.
- Hold no database, no file, no message payload. Only a routing table of live
  sockets.
- Transfer only the actual difference between devices, not a full dump.

## 2. Non-goals

- Persistence of any kind, including in-memory SQLite or an in-memory message
  store. If the design needs a store, we do not build it at all.
- Remembering anything a round said. The relay may read a round's structure
  while it passes through, but no history of data and no history of metadata —
  not even timestamps — outlives it.
- Replacing `wss://free.evoluhq.com` for production use.
- Quota, billing, metrics, multi-relay federation.
- Supporting a device that is offline while another device mutates data.

## 3. How it works

### 3.1 Protocol facts this design rests on

Verified in `node_modules/@evolu/common/src/local-first`:

1. **A client applies any inbound message, with no request/response
   correlation.** `Shared.ts:378` hands every WebSocket frame straight to
   `applyProtocolMessageAsClient` (`Shared.ts:399`). The relay may therefore
   push a message to a client at any moment, unsolicited.
2. **A client initiates sync itself on connect.** `Shared.ts:366` (`onOpen`)
   triggers `createProtocolMessageForSync` for every owner claimed on that
   socket. A join needs no probe from the relay.
3. **Request, Response and Broadcast share the same body.** `unwrap()`
   (`Protocol.ts:800`) always emits `[header][messages][ranges]`. Only the
   header prefix differs (`Protocol.ts:660`). Converting one message type into
   another is a byte splice of the first few bytes; messages and ranges pass
   through untouched.
4. **Both sides run the same reconciliation.** `sync()` (`Protocol.ts:1316`) is
   shared by `applyProtocolMessageAsClient` and `applyProtocolMessageAsRelay`.
   A client fed a `Response` carrying another client's ranges answers with
   exactly the messages that other client is missing, plus its own ranges — so
   one conversation reconciles both directions at once.
5. **A Broadcast ignores everything after the messages.**
   `applyProtocolMessageAsClient` returns at `Protocol.ts:1018`, before
   `decodeRanges`. A Broadcast may therefore carry a trailing ranges section
   and the client will simply not look at it.
6. **Messages are written before the write-key check** (`Protocol.ts:993`), so
   data delivery does not depend on a write key the relay does not have.
7. **Sync state is a placeholder.** `SyncState = 123` (`Shared.ts:286`), so
   there is no client-side sync indicator to satisfy or fake.
8. **Faking ranges to force another round is an established move in this
   protocol** — the client itself appends a random fingerprint range when its
   messages do not fit (`Protocol.ts:533`).

### 3.2 Routing rules

Per owner, the relay keeps a set of live sockets. For every inbound message
from socket `S` for owner `X`:

| Situation | Action |
| :-- | :-- |
| `X` has a partner socket `P` | Rewrite header to `Response(NoError)`, send the untouched body to `P`. |
| `X` has further sockets besides `S` and `P` | Rewrite header to `Broadcast`, send the untouched body to each of them. |
| `S` is the only socket for `X` | Reply to `S` with a bare `Response(NoError)`: no messages, no ranges. The client reads zero ranges, returns `NoResponse`, and considers itself synced. |

That is the whole protocol engine. The relay never decodes a message body, never
computes a fingerprint, never decides who needs what — the two paired clients do
the reconciliation between themselves and the third parties get a copy of
everything that crosses the wire.

### 3.3 Header rewriting

Fixed lengths, all from public exports:

```
version (1 byte for v1) | ownerId (16 B, Type.ts:7519) | messageType (1 B)
  Request:   hasWriteKey (1 B) + writeKey (16 B, Owner.ts:132) if the flag is 1
             + subscriptionFlag (1 B)
  Response:  errorCode (1 B)
  Broadcast: -
```

Rewrite = drop the old prefix, write the new one, append the rest verbatim.
Reject anything whose version is not 1 (`parseProtocolHeader`,
`Protocol.ts:383`, refuses it anyway).

The write key is dropped when a Request becomes a Response. That is fine: the
receiving client validates nothing and signs its own reply with its own key.

### 3.3b What the relay reads, and forgets

`inspect()` walks the `[messages][ranges]` body and returns two numbers: how
many messages and how many ranges the round carries. Nothing is decrypted (no
key here) and nothing is kept — the counts die with the round. They decide:

- **Whether to broadcast at all.** A pure reconciliation round carries no
  messages, and a Broadcast's ranges are ignored by the client, so forwarding
  one is pure noise. Most rounds of a sync are exactly that.
- **When a conversation is over.** No ranges means the sender wants no further
  round — a mutation push, or the last word of a sync. The pipe is released
  there and then, which is what frees the introduction lock without waiting for
  the timeout.
- **When a write key is required.** A round that carries messages must bring a
  matching key; one that only reconciles need not.

It mirrors Evolu's internal encoding (variable-length counts, delta millis,
run-length encoded counters and node ids, length-prefixed changes), which is
not exported and is pinned to protocol version 1. On anything unexpected it
returns `null` and every caller behaves as if it had not looked, so a format
change costs the three improvements above, never the sync. The test builds
rounds with Evolu's own encoder and compares, which is what fails loudly the
day the format moves.

### 3.4 Routing table and lifecycle

- Key the table by the **`roomId` in the URL**, settled before the upgrade
  (§4b). The owner is not in the address at all: the **first round settles it**
  and every round after it has to agree, which is the same shape as the write
  key gate (§3.6).
- One socket therefore carries one owner, but that follows from the address,
  not from Evolu: `Shared.ts` shares one socket per transport and lets several
  owner ids claim it (`createSharedResourceByKeyWithClaims`), so two owners
  pointed at one URL would share a socket. A room per owner means a URL per
  owner means a socket per owner. Keep it that way.
- The settled-owner rule is not a gate against an attacker — reaching a room
  takes the owner's secret. It is what turns an app that points two owners at
  one room into a visible drop instead of silently mixed data.
- A socket joins its owner on its first message, or on
  `SubscriptionFlags.Subscribe`; it leaves on `Unsubscribe` and on close.
- Nothing survives a disconnect. Nothing survives a restart. Clients reconnect
  and re-reconcile, which costs one fingerprint round trip.

### 3.5 Pairing

- On join, pair the newcomer with the **newest free** socket of that owner. Its
  own `onOpen` sync message starts the conversation; the relay only has to route
  it. Newest rather than oldest because a device whose network dropped leaves a
  socket that is still open here and will never answer again — after the outage
  both devices reconnect, and newest-first pairs those two fresh sockets with
  each other instead of piping the round into the leftover, where the answer
  would be lost and sync would look dead until something forced a reconnect.
- The local server also pings (`idleTimeout: 30`, `sendPings: true`), so such a
  leftover is reaped within ~30s instead of Bun's two-minute default — and the
  browser on the other end learns from its failing pong that the socket is gone
  and reconnects. On Cloudflare that liveness is the platform's job.
- **Only one pipe conversation per peer socket at a time.** A socket is a single
  channel with no conversation id, so two simultaneous pipes into the same peer
  would interleave and the relay could not tell whose reply is whose.
- **Introductions are serialized per owner.** A socket's *first* round waits
  until every pipe of that owner has gone quiet; later rounds never wait. That
  is what makes "every connected device holds the union" true when several
  devices connect at once: whoever answers a newcomer has already ingested the
  previous newcomer's data. Without it, three devices connecting together can
  leave the last one permanently missing the first one's data, because a device
  that has been told "you are in sync" never offers it again.
- A pipe ends the moment a round carries no ranges, so the lock is released by
  the protocol itself. The quiet timeout stays only as the fallback for a
  conversation that stalls mid-way.
- A waiting round is **held, not dropped**, and routed again as soon as the
  earliest pipe falls quiet. Both servers share that bookkeeping and only differ
  in the clock they arm: `setTimeout` locally, `ctx.storage.setAlarm` in the
  Durable Object.
- **A partner that does not answer within `answerTimeoutMs` loses the round.**
  It is offered to the next peer that has not had it, newest first, one peer at
  a time. A peer is never handed the same round twice, so the sweep terminates.
- **When the sweep runs out, the sender is told it is in sync** — the same
  answer a device connected alone gets, because operationally it is the same
  situation: nobody here can answer. The device may be behind; the next
  introduction by any peer repairs it, since reconciliation is two-way and its
  own data already went out as a Broadcast. Closing the socket to force a retry
  was the alternative and is not worth it: if every peer is unresponsive, the
  reconnect meets the same peers, and the pings reap them anyway.
- A late answer cannot create a second conversation on one socket: by then the
  sender is paired with its new partner, so the late round is not piped to it,
  and its data still arrives as a Broadcast.

### 3.6 Write-key gating

The relay cannot validate a write key against anything stored, but it must not
pipe arbitrary strangers into a live owner. Keep the **first write key seen for
an owner in memory** for as long as that owner has at least one socket, and
reject a Request whose key differs with `ProtocolErrorCode.WriteKeyError`. This
is memory, not persistence: it dies with the last connection.

## 4. Implementation checklist

Four layers, and only the bottom two hold the program:

- `src/wire.ts` is the byte layer: the header rewriting (§3.3) and `inspect()`
  (§3.3b). It reads and splices bytes and nothing else.
- `src/routing.ts` is `routeFor`, the pure routing decision, plus the
  hold/release bookkeeping and `nextWakeMs`. Arithmetic over peers and
  deadlines — it has no import at all.
- `src/relay.ts` is the relay itself — one owner's devices and the rounds
  moving between them: the write-key gate, the introduction, the pairing, the
  held rounds, the clock. It is written **once** and both hosts run it. It
  reaches its world through two small ports: a `RelaySocket` (`send`, `state`,
  `setState`) and a `RelayHost` (`sockets`, `wakeAt`, `now`, `log`), so it
  names no socket API, no timer and no runtime.
- `src/server.ts` is the local host adapter, run with `bun run src/server.ts`
  (`--port`, default 4000; `--cert`/`--key` for wss). `Bun.serve` provides the
  WebSocket server, so no new dependency.
- `src/worker.ts` plus `wrangler.toml` is the Cloudflare host adapter, see §4b.

One relay instance per room in both hosts, and a room is one owner — that is
what makes the two adapters thin. They differ only in who owns the map: a
Durable Object is addressed by room and holds exactly one, while the local
server holds a `Map<RoomId, Relay>` and forgets an entry once it reports
`isIdle()`. The adapters supply what genuinely differs and nothing else:

| | local (`server.ts`) | Cloudflare (`worker.ts`) |
| --- | --- | --- |
| per-socket state | `socket.data` | the socket attachment |
| clock | `setTimeout` | `ctx.storage.setAlarm` |
| socket list | the room's array | `ctx.getWebSockets()` |
| one relay per room | an entry in a `Map` | the object itself |

Both are addressed the same way, so both settle a socket's room and owner before
the upgrade; the difference is only who holds the map. That is also why the
local server no longer multiplexes owners over one socket: the owner is in the
address, not only in each header.
- `@evolu/common` is the only dependency: `parseProtocolHeader`, `MessageType`,
  `ProtocolErrorCode`, `SubscriptionFlags`, `ownerWriteKeyLength`,
  `nodeIdBytesLength`, `idBytesTypeValueLength`, `OwnerId` and
  `createProtocolMessageBuffer`. Nothing platform-specific is imported, so the
  same code runs under Bun and in a Worker.
- State: the sockets of an owner, a `SocketState` each (id, pipe partner, when
  that pipe last moved, introduced, gone), a per-owner first write key, and the
  rounds waiting for a partner. Nothing else, and none of it on disk.
- Log one event per connect, join, pipe start, pipe end, disconnect. The relay
  writes through Evolu's `Console`, handed to it by the host and
  taken down to a `child` per owner, so both hosts get the same lines and each
  says whose it is — `[<ownerId>] pipe { from: 2, to: 1 }`. Fields are
  passed as they are rather than flattened, so whatever renders them can. This
  is an experiment; being able to read what happened matters more than being
  quiet, so the lifecycle is `info` and only the per-round chatter is `debug`.
- A `ponytail:` comment on every shortcut listed in §6.

## 4b. Cloudflare Durable Objects

The same relay, hosted. A Worker maps the URL path to one Durable Object per
room and the object is where that room's devices meet.

- **Address**: `wss://<host>/<roomId>`, and nothing else. The Worker validates
  the path with `parseRoomId` and calls `env.RELAY.idFromName(roomId)`, so
  every device of one owner lands in the same object and two owners never share
  one. The owner is never in the address — every message carries it in its
  header, so the object reads it from there (`readOwnerId`) and the first round
  settles whose room it is.
- **What a room id may be**: one URL path segment of `[A-Za-z0-9_-]`, at most
  64 characters — the Base64Url alphabet Evolu's own `Id` is written in, so the
  derived id below fits with nothing to encode. Those are the only three things
  the relay itself needs: one segment, nothing that would forge a log line, and
  a bound, because the id becomes a Durable Object's name and the prefix of
  every line that room writes. Everything else is the app's business, including
  choosing something short and guessable; the relay cannot tell and does not
  try. A refused address is answered `400` with the reason, and the reason is
  logged too, because a browser's WebSocket API shows neither.
- **Why the room is not just the owner id**: an owner id identifies an owner to
  anyone who sees it, and Evolu's own docs say to share it only with a relay
  that must verify access (`Owner.ts:255`). A room id derived from the secret
  is unguessable without it, so an attacker who learns an owner id can no
  longer find the room, squat its write-key slot or collect its metadata. It is
  not authentication — see §6 — and the relay treats it as opaque routing.
- **Why an object per room**: the routing table is the whole program, and
  serverless instances cannot share memory. A Durable Object is the one
  Cloudflare primitive that gives two live sockets the same single-threaded
  place to meet.
- **Still no storage**: no rows, no SQLite. The only storage API used is
  `setAlarm`, the clock that routes a held round once a pipe goes quiet. The
  only state that survives hibernation is what `serializeAttachment` keeps per
  socket — which is exactly `SocketState`, so the attachment codec *is* the
  adapter's `state`/`setState`.
- **No timers**: a pipe is released by checking its age at routing time
  (`routeFor`), not by a `setTimeout` that hibernation would kill.
- **Deploy**: `bun run deploy` (`wrangler deploy`).
  The class is registered as a SQLite-backed migration because those are the
  ones the free plan offers; it never writes to it.
- **App side**: the room id is the whole URL the app stores, passed to
  `transports` as it is:

  ```ts
  const roomId = idBytesToId(
    IdBytes.orThrow(
      createSlip21(secret, ["evolu-live-relay", "RoomId"]).slice(0, 16)
    )
  )
  transports: [{ type: "WebSocket", url: `wss://<host>/${roomId}` }]
  ```

  Not `createOwnerWebSocketTransport`: it appends `?ownerId=`
  (`Owner.ts:485`), which this relay has no use for and which would put the
  owner id back into URLs, logs and devtools. Evolu opens the socket at
  `transport.url` verbatim (`Shared.ts:363`), so a plain literal is all it
  takes.

  That is the recipe Evolu uses for the owner id itself, under a different
  SLIP-21 label, so the room id is a sibling of the owner id rather than
  derived from it: every device reaches it from the mnemonic
  (`mnemonicToOwnerSecret`) and nobody without the mnemonic can. The relay
  never derives it and does not care how it was made.

## 5. Verification

`src/routing.test.ts` covers `routeFor` — who gets the round when a device is
alone, when a pipe is running, when every peer is busy, when an introduction
has to wait, when a pipe has gone quiet, when a peer has already been tried,
and when the sweep runs out — plus `nextWakeMs`, which is what a host arms its
clock with. It is a pure function, so both hosts are checked at once.

`src/wire.test.ts` checks `inspect()` against rounds built with Evolu's own
encoder, and that a truncated one returns `null`. That is the alarm for the day
the wire format moves.

`src/relay.test.ts` drives `src/relay.ts` against a fake host: sockets that
only collect what was sent to them, and a clock moved by hand. Because both
hosts run that exact code, this is where the state machine is pinned down —
who becomes a peer (a socket that connected but never synced is not one, and
neither is one whose write key was refused), what a round without ranges does
to a pipe, what an unsubscribe and a close do, that a closing socket takes its
held rounds and its partner's pipe with it, that a round for another owner is
dropped, and that a silent partner is retried and then swept past. The sweep
costs a few milliseconds here because the clock is fake, so the timeouts can be
exercised properly rather than waited out.

`src/server.test.ts` spawns the local server on an ephemeral port and drives it
over a real WebSocket with real protocol messages built by Evolu's own encoder.
It is the end-to-end check on the Bun adapter — the upgrade, the frames, the
real `setTimeout` — over the same ground: a lone device is answered "in sync", a
peer is piped the request re-headered as a Response with the body byte-identical,
a third socket gets the same body as a Broadcast, a round held while every peer
is busy is delivered once a pipe goes idle, a silent partner is retried against
the next peer and the sender is finally told it is in sync, and a second write
key for one owner is rejected. `bun run test` runs all four files.

Note the sockets in that test sync on connect, because that is what a real
client does (`Shared.ts:366`) and because it is how the relay learns which owner
a socket belongs to. A socket that connects and stays silent is invisible to it.

The Cloudflare glue — `WebSocketPair`, `acceptWebSocket`, attachments — is not
covered; that would need `@cloudflare/vitest-pool-workers` and a second Vitest
project. It is now only glue, though: everything it wires up is covered by
`relay.test.ts`. `bunx wrangler deploy --dry-run` checks that it still bundles,
and CI runs it; run `wrangler dev` and watch two devices converge for the rest.

Two live Evolu clients converging through the relay is **not** covered and stays
a manual step: the local server is a Bun server, a node client needs
`@evolu/nodejs` with `better-sqlite3`, and two clients in one process share
Evolu's broadcast channel, so an in-process test could pass without the relay
moving anything. Point two real devices at it instead.

Wiring an app at it is manual too. Browsers only allow `wss://` from an https
page, so the local server wants a certificate (`--cert`, plus `--key` unless one
PEM holds both).

**The certificate must be genuinely trusted, not click-through accepted.** Evolu
opens every sync WebSocket inside its SharedWorker, and a SharedWorker has no
tab behind it, so a browser cannot ask there: the TLS handshake is refused and
the connection fails silently, with Evolu retrying on backoff and no error
anywhere. Measured in Chrome against this relay with a self-signed certificate:
page `open ok`, dedicated worker `open ok`, **SharedWorker `error`**, while the
same SharedWorker reaches a publicly trusted relay fine. `mkcert` is the short
way out — its local CA is installed into the system and into the browsers' NSS
stores, so nothing has to be clicked:

```
mkcert -install
mkcert -key-file key.pem -cert-file cert.pem localhost 127.0.0.1 ::1
bun run src/server.ts --cert cert.pem --key key.pem
```

A phone needs that CA too (`$(mkcert -CAROOT)/rootCA.pem`), or it has to reach
the relay on localhost through a port forward (`adb reverse tcp:4000
tcp:4000`). On Cloudflare none of this applies: the certificate is public.

## 6. Known risks we accept and will not fix

- **It stands on undocumented behaviour.** Clients process unsolicited messages
  with no correlation (`Shared.ts:378`). If Evolu ever adds a sync state machine
  or request/response pairing, the relay breaks. It is an experiment; the
  protocol header itself already lists client-to-client sync as a future feature
  (`Protocol.ts:6`).
- **No catch-up.** A device that was offline while another mutated gets nothing
  until both are online together. This is the defining property, not a bug.
- **A swept round can still end with nothing.** If no peer answers, the sender
  is told it is in sync while it may be behind. It is repaired by the next
  introduction of any peer, but a fleet where nobody ever reconnects or writes
  again would stay split. Forcing a reconnect (closing the socket) is the
  escalation if that ever shows up.
- **A stalled conversation still releases the lock on a timeout.** The normal
  ending is exact now (a round with no ranges), but a conversation that simply
  stops mid-way is indistinguishable from a slow one, so after `pipeIdleMs` the
  next newcomer may be answered by a peer holding only part of the data. Rare,
  and healed by the next sync.
- **A mutation push can cut a conversation short.** A device that is piped with
  a partner may send an unrelated round at any moment — the user typed
  something — and the relay cannot tell that from the answer the partner was
  waiting for: both are Requests from a paired socket, and both may carry no
  ranges. The partner's round is dropped and it receives the push as a
  Broadcast, which is also how a conversation ends normally, so nothing breaks
  — its ranges simply go unreconciled until the next sync.
- **`inspect()` is copied from Evolu's internals.** It is pinned to protocol
  version 1 and can drift silently on an upgrade. It fails safe — a `null` puts
  the relay back to the behaviour it had without it — and the encoder test is
  the alarm.
- **Pipe correlation is heuristic.** Serialising by "peer went quiet" is a
  timeout, not a protocol guarantee. Two joins racing on the same peer can still
  interleave under bad timing; the result is redundant rounds, not corruption.
- **Inside a room, the first write key seen wins** for the lifetime of its
  connections, because the relay has no way to know which one is right —
  `writeKey` is a symmetric SLIP-21 sibling, so there is nothing public to
  check it against. Reaching a room now takes the secret, which is what makes
  this tolerable; before the room id it was enough to know an owner id.
  Whoever does get in sees ciphertext and metadata, never data: payloads stay
  end-to-end encrypted and a client quarantines what it cannot decrypt
  (`Db.ts:209`).
- **The room id is not authentication.** The relay verifies nothing — it cannot,
  for the reason above — it only routes an opaque token. The security it buys
  is that the address is unguessable, not that a connection is proven. A
  malicious relay still sees every owner id (it is in each message header),
  every write key, and the shape of everything that flows.
- **Fan-out to third parties is unconditional.** Every device receives every
  message that crosses the relay, including ones it already has. Idempotent,
  wasteful, fine at three devices.
- **A held round lives in memory, so Cloudflare can lose it to hibernation.**
  Both servers hold a round that has no partner yet and route it when a pipe
  falls quiet; writing those bytes to storage would turn the object into the
  database this experiment refuses to be. If the object hibernates in that
  window, the device catches up on its next sync instead.
- **On Cloudflare the write key resets when the object hibernates.** The gate is
  in memory by design, so after a wake the first key seen wins again.
- **No backpressure, no size limits, no rate limiting.** A hostile client can
  make the relay forward as fast as it can write. Local experiment only.
- **The TLS certificate is a throwaway.** It is the dev server's self-signed
  `CN=example.org`, trusted by nobody; every device has to click through it
  once. Fine for an experiment on a localhost port, not for anything reachable.

## 7. Possible improvements later

- **Skip the pipe when nothing changed.** Today every join costs one fingerprint
  round trip even when both sides are identical. Cheap already; measure before
  optimising.
- **Per-socket timestamp memory.** Remembering only the `TimestampBytes` a
  socket has sent (16 B each, `testFingerprintTimestamps`, `Storage.ts:1097`)
  would let the relay answer "I already have these" and act as a real
  non-initiator when no partner is connected — that is what would make a lone
  device's push meaningful instead of discarded. It is still not persistence,
  but it is state, so it needs a separate decision.
- **Conversation ids.** If simultaneous joins on one peer ever become real, the
  clean fix is per-owner sharding of conversations across different peers rather
  than a timeout.
- **A probe to force a resync** — an unsolicited `Response` carrying
  `Timestamps([], InfiniteUpperBound)` makes a client dump its entire database
  (`Protocol.ts:1445`, empty needed-set → it sends everything, then
  `Protocol.ts:1529` ends the round). Not needed for the normal flow, but it is
  the recovery hammer if a pairing goes wrong. Note the ceiling: a dump larger
  than the 1 MB message cap restarts from the beginning each round unless the
  timestamp memory above exists.
- **A queue on Cloudflare.** If three devices syncing at once turns out to
  matter, `ctx.storage.setAlarm()` plus the round in storage would restore the
  local server's behaviour — at the cost of writing user data to disk, which is
  what this experiment set out to avoid.
- **Owner allow-list.** `RelayConfig.isOwnerAllowed` (`Relay.ts:133`) is the
  shape to copy if this ever needs to be reachable from outside localhost.
- **Two proxies, one owner.** Out of scope and probably pointless: with no
  storage there is nothing to federate.
