import { err, ok, type Result } from "@evolu/common"
import { defineError } from "./error.ts"

/**
 * The address of one room, and the whole of what the relay knows about it.
 *
 * It is an opaque token: the relay never derives it, never looks inside it and
 * never checks it against anything (DESIGN.md §4b). What is checked is only
 * what the relay itself needs — that it is one URL path segment, that it can
 * go in a log line, and that it is bounded, because it becomes the name of a
 * Durable Object and the prefix of every line that room writes.
 *
 * Short ids are allowed and unwise. The only security a room id has is that it
 * cannot be guessed, so a real one should be derived from the owner's secret —
 * see README. The relay cannot tell the difference and does not try.
 */
declare const roomIdBrand: unique symbol
export type RoomId = string & { readonly [roomIdBrand]: true }

/**
 * Base64Url's alphabet, which is what Evolu's own `Id` is written in, so the
 * derived room id in the README fits with nothing to encode. Everything
 * outside it either needs percent-encoding in a URL or has no business in a
 * log line.
 */
const allowedCharacters = /^[A-Za-z0-9_-]+$/

/** Long enough for any sane token; short enough to bound a log line. */
export const roomIdMaxLength = 64

const createRoomIdError = defineError("RoomIdError")<{
  readonly reason: string
}>()
export type RoomIdError = ReturnType<typeof createRoomIdError>

export const parseRoomId = (value: string): Result<RoomId, RoomIdError> => {
  if (value.length === 0) {
    return err(createRoomIdError({ reason: "the room id is missing" }))
  }

  if (value.length > roomIdMaxLength) {
    return err(
      createRoomIdError({
        reason: `a room id may be at most ${roomIdMaxLength} characters, this one is ${value.length}`,
      })
    )
  }

  if (!allowedCharacters.test(value)) {
    return err(
      createRoomIdError({
        reason: "a room id may only contain letters, digits, - and _",
      })
    )
  }

  return ok(value as RoomId)
}
