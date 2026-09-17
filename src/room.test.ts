import { expect, test } from "vitest"

import { parseRoomId, roomIdMaxLength } from "./room.ts"

/**
 * The relay treats a room id as opaque, so this is not about what a good one
 * is — it is only about what the relay itself cannot take: something that is
 * not one URL path segment, something that has no business in a log line, and
 * something unbounded.
 */

const reasonOf = (value: string) => {
  const result = parseRoomId(value)
  return result.ok ? null : result.error.reason
}

test("an ordinary token is a room id", () => {
  for (const value of [
    "JN2A6YMaduSaWZotx-f_PQ", // what the derivation in the README produces
    "abc", // short and guessable, which is the caller's business
    "a",
    "A-Za-z0-9_-",
    "x".repeat(roomIdMaxLength),
  ]) {
    expect(parseRoomId(value)).toEqual({ ok: true, value })
  }
})

test("a missing room id says so", () => {
  expect(reasonOf("")).toBe("the room id is missing")
})

test("anything that is not one plain path segment is refused", () => {
  // A second segment, percent-encoding, a query that was not stripped, and a
  // newline that would forge a log line.
  for (const value of ["rooms/one", "a%2Fb", "room?ownerId=x", "room\nopen"]) {
    expect(reasonOf(value)).toBe(
      "a room id may only contain letters, digits, - and _"
    )
  }
})

test("an unbounded room id is refused, and the reason says how long it was", () => {
  const tooLong = "x".repeat(roomIdMaxLength + 1)

  expect(reasonOf(tooLong)).toBe(
    `a room id may be at most ${roomIdMaxLength} characters, this one is ${tooLong.length}`
  )
})
