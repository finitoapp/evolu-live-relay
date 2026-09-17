import { expect, test } from "vitest"

import {
  answerTimeoutMs,
  nextWakeMs,
  type Peer,
  type PendingRound,
  pipeIdleMs,
  resolvePending,
  routeFor,
} from "./routing.ts"

/**
 * `routeFor` and `nextWakeMs` are pure, so this covers both hosts at once: who
 * gets the round when a device is alone, when a pipe is running, when every
 * peer is busy, when an introduction has to wait, when a pipe has gone quiet,
 * when a peer has already been tried, and when the sweep runs out.
 */

const now = 1_000_000

const peer = (
  id: number,
  partnerId: number | null = null,
  pairedAt = 0
): Peer => ({
  id,
  partnerId,
  pairedAt,
})

test("a lone device is told it is in sync", () => {
  expect(routeFor(1, [peer(1)], now)).toEqual({
    partnerId: null,
    listenerIds: [],
    inSync: true,
    exhausted: false,
    hold: false,
  })
})

test("the only peer becomes the pipe partner", () => {
  expect(routeFor(2, [peer(1), peer(2)], now)).toEqual({
    partnerId: 1,
    listenerIds: [],
    inSync: false,
    exhausted: false,
    hold: false,
  })
})

test("a running pipe keeps its partner and the rest only listen", () => {
  const peers = [peer(1, 2, now), peer(2, 1, now), peer(3)]
  expect(routeFor(1, peers, now)).toEqual({
    partnerId: 2,
    listenerIds: [3],
    inSync: false,
    exhausted: false,
    hold: false,
  })
})

test("a third device gets no partner while both peers are busy", () => {
  const peers = [peer(1, 2, now), peer(2, 1, now), peer(3)]
  expect(routeFor(3, peers, now)).toEqual({
    partnerId: null,
    listenerIds: [1, 2],
    inSync: false,
    exhausted: false,
    hold: true,
  })
})

test("an introduction waits while another one is still running", () => {
  const peers = [peer(1, 2, now), peer(2, 1, now), peer(3), peer(4)]
  // Peer 3 is free, but 4 is a newcomer, so it waits for the running pipe.
  expect(routeFor(4, peers, now, { isJoin: true })).toEqual({
    partnerId: null,
    listenerIds: [1, 2, 3],
    inSync: false,
    exhausted: false,
    hold: true,
  })
  // A later round from the same socket does not wait.
  expect(routeFor(4, peers, now, { isJoin: false })).toEqual({
    partnerId: 3,
    listenerIds: [1, 2],
    inSync: false,
    exhausted: false,
    hold: false,
  })
  // Once the pipe is quiet the newcomer goes through.
  expect(routeFor(4, peers, now + pipeIdleMs, { isJoin: true })).toEqual({
    partnerId: 3,
    listenerIds: [1, 2],
    inSync: false,
    exhausted: false,
    hold: false,
  })
})

test("a pipe quiet for the idle window frees its peers", () => {
  const peers = [peer(1, 2, now), peer(2, 1, now), peer(3)]
  expect(routeFor(3, peers, now + pipeIdleMs)).toEqual({
    partnerId: 2,
    listenerIds: [1],
    inSync: false,
    exhausted: false,
    hold: false,
  })
})

test("the newest free peer is picked, so a leftover socket is skipped", () => {
  // Peer 1 is a device whose network dropped: still here, never answers.
  const peers = [peer(1), peer(2), peer(3)]
  expect(routeFor(3, peers, now)).toEqual({
    partnerId: 2,
    listenerIds: [1],
    inSync: false,
    exhausted: false,
    hold: false,
  })
})

test("a partner that disconnected mid-pipe is replaced", () => {
  const peers = [peer(1, 9, now), peer(2)]
  expect(routeFor(1, peers, now)).toEqual({
    partnerId: 2,
    listenerIds: [],
    inSync: false,
    exhausted: false,
    hold: false,
  })
})

test("a peer that did not answer is not offered the round again", () => {
  const peers = [peer(1), peer(2), peer(3)]
  expect(routeFor(3, peers, now, { triedIds: [2] })).toEqual({
    partnerId: 1,
    listenerIds: [2],
    inSync: false,
    exhausted: false,
    hold: false,
  })
})

test("when every peer has been tried the sender is told it is in sync", () => {
  const peers = [peer(1), peer(2)]
  expect(routeFor(2, peers, now, { triedIds: [1] })).toEqual({
    partnerId: null,
    listenerIds: [],
    inSync: true,
    exhausted: true,
    hold: false,
  })
})

const pendingRound = (
  senderId: number,
  deadline: number | null
): PendingRound => ({
  senderId,
  message: new Uint8Array(),
  isJoin: false,
  triedIds: [],
  deadline,
})

test("the clock wakes on the nearest deadline or quiet pipe", () => {
  const peers = [peer(1, 2, now), peer(2, 1, now)]

  expect(nextWakeMs(peers, [], now)).toBe(null)

  // An answer is due before the pipe falls quiet.
  expect(nextWakeMs(peers, [pendingRound(1, now + answerTimeoutMs)], now)).toBe(
    answerTimeoutMs
  )

  // A round with no partner yet waits for the pipe instead.
  expect(nextWakeMs(peers, [pendingRound(3, null)], now)).toBe(pipeIdleMs)

  // Nothing left to wait for, so it is worth trying straight away.
  expect(nextWakeMs([peer(1)], [pendingRound(3, null)], now)).toBe(0)
})

test("an answer ends the round that was waiting for one", () => {
  // The same sender can have a round still held for want of a partner and a
  // round handed to one; only the second is what an answer ends.
  const held = pendingRound(1, null)
  const awaiting = pendingRound(1, now + answerTimeoutMs)
  const pending = [held, awaiting]

  resolvePending(pending, 1)
  expect(pending).toEqual([held])

  // With nothing awaiting an answer, the held round is the one that goes.
  resolvePending(pending, 1)
  expect(pending).toEqual([])
})
