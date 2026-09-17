# Agent Guide

## What this is

A WebSocket relay for Evolu that **stores nothing**. It pairs the connected
devices of one owner while they are online together and pipes their sync rounds
at each other; a device connected alone is told it is in sync and its data is
dropped. It is written once and hosted twice: locally on Bun, and on Cloudflare
as a Durable Object.

`DESIGN.md` is the specification and it is current — §3 for the protocol facts
and the routing rules, §4/§4b for the layering, §5 for what the tests do cover
and what stays manual, §6 for the risks that are accepted on purpose. Read the
relevant section before changing routing, pairing, or the byte layer; a change
that contradicts it is a change to it, so update the document in the same pass.

## Commands

- `bun run check` before handing work back. It is `check:lint` (Biome) +
  `check:ts` (`tsc -b`, tests included) + `check:tests` (Vitest); run those
  individually while narrowing a failure. The same three run in CI
  (`.github/workflows/code-quality.yml`).
- `bun run format` applies Biome's fixes; `bun run test:watch` reruns Vitest on
  change.
- `bun run start` runs the local server (`--port`, default 4000;
  `--cert`/`--key` for wss). `bun run dev:worker` is `wrangler dev`.
- `bunx wrangler deploy --dry-run` bundles the Worker without touching
  Cloudflare and needs no account or token. Run it after changing `worker.ts`
  or `wrangler.toml` — `tsc` does not catch everything the bundler does, and CI
  runs it as its own job.
- Two live Evolu clients converging through the relay is not automated and will
  not be; see `DESIGN.md` §5 for why, and for the certificate trap that makes a
  browser fail silently.

## Project Rules

- Write all code, comments, commit messages, and documentation in English.
- Commit messages follow Conventional Commits: `type(scope): imperative
  summary`, lowercase, no trailing period. The subject says what changed and the
  body says why — put the reasoning there, not in the subject.
- **Store nothing.** No database, no file, no in-memory message store, no
  history of data or of metadata — not even timestamps — outlives a round. This
  is the whole point of the project, not a performance preference: if a change
  needs a store, it does not get built (`DESIGN.md` §2). The one storage API in
  use is Cloudflare's `setAlarm`, which is a clock, and it stays that way.
- **`@evolu/common` is the only dependency**, and only its public exports.
  Adding a runtime dependency is a design decision, not a convenience: say why
  in the commit body. Dev dependencies are Biome, TypeScript, Vitest, Wrangler
  and `@types/node`. Every version in `package.json` is pinned exactly, with no
  range prefix.
- Evolu itself stays untouched. Where this code mirrors an Evolu internal that
  is not exported — `inspect()` in `wire.ts` does — it must fail safe and say so
  (see the `ponytail:` rule below).
- Mark every deliberate shortcut with a `ponytail:` comment, and list it in
  `DESIGN.md` §6. Three exist today: the write-key comparison is plain equality,
  `maxHeldRounds` is a fixed cap, and `inspect()` is pinned to protocol version
  1. A `ponytail:` comment is not a TODO — it is an accepted risk with a reason,
  so do not "fix" one without changing §6 too.
- Comments explain **why**, not what. The existing ones carry the reasoning that
  is not recoverable from the code — why the newest free peer wins, why the
  write key never reaches a Broadcast, why a held round is not written down.
  Match that density; do not strip it, and do not pad it with restatements of
  the line below.
- Use Bun for dependency management and scripts. Use Biome for linting and
  formatting.
- Keep the project TypeScript-first and preserve strict compiler settings.

## Project Structure

Four layers. A file may only depend downwards, and the two bottom ones hold the
whole program:

- `src/wire.ts` — the byte layer. Header offsets, `readWriteKey`,
  `isUnsubscribe`, the Request→Response/Broadcast re-headering, and `inspect()`,
  which walks a round's framing without decoding it. No state, no I/O. It
  imports `@evolu/common` and nothing local.
- `src/routing.ts` — who gets a round and when to look at it again. `routeFor`,
  the held-round bookkeeping, `nextWakeMs`. Pure arithmetic over peers and
  deadlines; **it has no import at all**, and keeping it that way is the test
  that the layer is still honest.
- `src/relay.ts` — the relay itself, built on those two: the write-key gate, the
  introduction, the pairing, the held rounds, the clock. One instance per owner.
  It must name no socket API, no timer and no runtime; it reaches its world only
  through `RelayHost` (`openSockets`, `wakeAt`, `now`, `console`) and
  `RelaySocket` (`send`, `state`, `setState`).
- `src/server.ts` (Bun) and `src/worker.ts` (Cloudflare Durable Object) — the
  host adapters, and the two entry points. They supply the ports and nothing
  else.

Where a change belongs follows from that: routing behaviour goes in
`relay.ts`/`routing.ts` and is then true in both hosts at once. If you find
yourself adding the same logic to both adapters, it belongs one layer down —
that duplication is exactly what this structure was built to remove. An adapter
may hold only what genuinely differs between the two runtimes:

| | local (`server.ts`) | Cloudflare (`worker.ts`) |
| --- | --- | --- |
| per-socket state | `socket.data` | the socket attachment |
| clock | `setTimeout` | `ctx.storage.setAlarm` |
| socket list | the room's array | `ctx.getWebSockets()` |
| one relay per room | an entry in a `Map` | the object itself |

Both hosts are addressed the same way — `/<roomId>?ownerId=<ownerId>` — so a
socket belongs to exactly one room and one owner for its whole life, settled
before the upgrade. The room id is opaque routing: the relay never derives it
and `Id.is` is the whole of its validation. What it should be derived from, and
what that does and does not buy, is DESIGN.md §4b and §6 — an unguessable
address, not authentication, so never write anything that treats a reachable
room as a proven one.

Other files:

- `src/error.ts` — `defineError`, the typed-error factory the rule above uses.
  `src/log.ts` — `createRelayConsole`, the console each adapter's Run is built
  with. Neither belongs to a layer; everything may import them.
- `wrangler.toml` — the Worker. The Durable Object class is `EvoluLiveRelay`
  and its name is pinned by the `v1` migration, so renaming it is a migration
  (`renamed_classes`), not a rename.
- `DESIGN.md` — the specification. `README.md` — what it is and how to run it.
- Neither host has a type package (no `bun-types`, no
  `@cloudflare/workers-types`). Each adapter hand-declares the subset of its
  runtime it uses, at the top of the file. Extend that declaration when you need
  more of the API; declare it honestly (`setAlarm` returns a `Promise`, so it is
  typed as one) rather than widening it to `any`.

## Tests

Each layer is tested where it lives, and `bun run test` runs all four files.

- `src/routing.test.ts` — `routeFor`, `nextWakeMs` and the held-round
  bookkeeping as pure functions, so both hosts are covered at once.
- `src/wire.test.ts` — `inspect()` against rounds built with Evolu's own
  encoder. This is the alarm for the day the wire format moves; keep it.
- `src/relay.test.ts` — the state machine against a fake host: sockets that only
  collect what was sent to them, and a clock moved by hand. **This is where
  behaviour changes get their test.** A sweep that costs seconds over a real
  socket costs nothing here, so timeouts are exercised rather than waited out.
- `src/server.test.ts` — the Bun adapter end to end, over a real WebSocket on an
  ephemeral port, with messages built by Evolu's own encoder.

Conventions:

- Build protocol messages with Evolu's own encoder (`createProtocolMessageBuffer`
  and friends), never by hand-assembling bytes. A test that hardcodes a byte
  layout stops being a check on the format and becomes a copy of it.
- Where a suite needs an owner, mint a fresh one per test
  (`createAppOwner(createOwnerSecret(...))` in `relay.test.ts` and
  `server.test.ts`), so tests never share relay state. `wire.test.ts` uses
  Evolu's fixed `testAppOwner`, because it only reads bytes back.
- Test names are sentences about behaviour ("a lone device is told it is in
  sync"), not about function names.
- Before trusting a new test, check it fails against the behaviour it forbids.
  Several of these pin down one specific decision and would pass by accident.

## Action and Error Patterns

- Dependencies are injected, never ambient — the `D` of an Evolu `Task`, for a
  synchronous state machine. `relay.ts` reaches its runtime only through
  `RelayHost`/`RelaySocket`: no `Date.now()`, no `setTimeout`, and never the
  global `console`.
- Each adapter builds those dependencies at its composition root, at the top of
  the file, and passes them down: `host.console` is `createRelayConsole()` from
  `src/log.ts` — Evolu's default console plus the formatter it leaves to the
  caller, without which the native output drops the path. The relay takes a
  `child(ownerId)` of it, so an owner's lines prefix themselves instead of every
  call site repeating the owner. Levels carry meaning: `info` is the lifecycle
  DESIGN.md §4 asks for (open, join, pipe, done, leave, close), `debug` is the
  per-round chatter that is off by default, `warn` is a refused write key or a
  round the reader could not parse.
- Expected failures return a `Result` (`ok`/`err`/`trySync`/`tryAsync` from
  `@evolu/common`); throwing is for programmer errors. A frame the relay cannot
  use has nobody to return an error to — log the reason and drop it.
- Define every error with `defineError` from `src/error.ts` and export its type
  via `ReturnType`: `const createInspectError = defineError("InspectError")<{
  readonly reason: string }>()` with `export type InspectError = ReturnType<
  typeof createInspectError>`. Carry what the caller needs to report — a path, a
  cause — not a pre-formatted sentence.
- No `Task` and no `createRun`. The relay runs synchronously once per frame and
  has nothing to await, abort or supervise, and in a Durable Object that
  synchronous path is also the one that hibernates cleanly — so a `Run` would be
  a fiber supervisor held in order to hand over one logger, and it doubled the
  Worker bundle when it was tried (24 KiB gzip against 14). Reach for
  `createRun` when a dependency actually needs it — real async work with
  failures to propagate — and thread `run` down in place of `RelayHost`; until
  then the adapter passing its deps in is the whole of it.

## TypeScript Rules

- Imports of local modules carry the `.ts` extension (`./routing.ts`);
  `allowImportingTsExtensions` and `verbatimModuleSyntax` are on, so
  `import type` is required for type-only imports and Biome enforces it.
- Prefer immutability: `const` unless reassignment is required, `readonly`
  fields, `ReadonlyArray<T>` for inputs and read-only collections. Use `T[]`
  where the code deliberately mutates — the `pending` array is spliced in place
  on purpose.
- Prefer `interface` for object shapes; `type` for unions, intersections and
  aliases.
- No `any` — Biome errors on it. Narrow from `unknown` instead. Outside the
  tests there are exactly two casts, both in `worker.ts` and both at a runtime
  boundary with no type to give: `deserializeAttachment() as SocketState`, and
  the Workers-only `webSocket` field on a 101 response. Keep it that way.
- Strict equality only. `noDoubleEquals` is configured with
  `ignoreNull: false`, so even `value == null` is an error; write
  `value === undefined` / `value !== null`.
- `noUncheckedIndexedAccess` is on, so guard indexed reads rather than reaching
  for `!`. Avoid non-null assertions.
- `noUnusedLocals`, `noUnusedParameters` and Biome's `noUnusedImports` /
  `noUnusedVariables` are all errors: dead code does not survive a commit.
- Prefer named exports. The one default export is a framework boundary: the
  Worker's `fetch` handler, which is the shape Cloudflare requires.
- Prefer `async`/`await` over `.then(...)` chains. Where a promise is genuinely
  fire-and-forget — the Durable Object's `setAlarm`, because the relay is
  synchronous — attach a `.catch` that logs, never leave it floating.
