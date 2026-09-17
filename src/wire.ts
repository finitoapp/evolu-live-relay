import { idBytesTypeValueLength, type Result, trySync } from "@evolu/common"
import {
  createProtocolMessageBuffer,
  MessageType,
  nodeIdBytesLength,
  type OwnerId,
  ownerWriteKeyLength,
  ProtocolErrorCode,
  SubscriptionFlags,
} from "@evolu/common/local-first"
import { defineError } from "./error.ts"

/**
 * The byte layer: everything the relay knows about what a round looks like on
 * the wire. See DESIGN.md §3.3.
 *
 * Request, Response and Broadcast carry the same `[messages][ranges]` body and
 * differ only in their header prefix, so routing a round from one device to
 * another is a byte splice — nothing here decodes a message or a range, and
 * nothing here holds state. Who a round goes to is routing.ts; relay.ts puts
 * the two together.
 */

const versionLength = 1
const messageTypeOffset = versionLength + idBytesTypeValueLength
const requestFlagsOffset = messageTypeOffset + 1

/** Not exported by Evolu, so it is derived from the value. */
export type ProtocolErrorCodeValue =
  (typeof ProtocolErrorCode)[keyof typeof ProtocolErrorCode]

/**
 * Where the `[messages][ranges]` body starts in a Request: past the write key
 * flag, the optional write key itself, and the subscription flag.
 */
export const requestBodyOffset = (message: Uint8Array): number =>
  requestFlagsOffset +
  1 +
  (message[requestFlagsOffset] === 1 ? ownerWriteKeyLength : 0) +
  1

const toHex = (bytes: Uint8Array) =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")

export const readWriteKey = (message: Uint8Array): string | null =>
  message[requestFlagsOffset] === 1
    ? toHex(
        message.subarray(
          requestFlagsOffset + 1,
          requestFlagsOffset + 1 + ownerWriteKeyLength
        )
      )
    : null

export const isUnsubscribe = (
  message: Uint8Array,
  bodyOffset: number
): boolean => message[bodyOffset - 1] === SubscriptionFlags.Unsubscribe

/**
 * Re-headers a Request as a Response or a Broadcast. Version and owner id are
 * copied verbatim and the body is never decoded.
 */
const withHeader = (
  message: Uint8Array,
  bodyOffset: number,
  messageType: number,
  errorCode: number | null
) => {
  const body = message.subarray(bodyOffset)
  const prefixLength = messageTypeOffset + 1 + (errorCode === null ? 0 : 1)
  const out = new Uint8Array(prefixLength + body.length)
  out.set(message.subarray(0, messageTypeOffset))
  out[messageTypeOffset] = messageType
  if (errorCode !== null) out[messageTypeOffset + 1] = errorCode
  out.set(body, prefixLength)
  return out
}

/** For the pipe partner, which answers it with the diff the sender is missing. */
export const toResponse = (message: Uint8Array, bodyOffset: number) =>
  withHeader(
    message,
    bodyOffset,
    MessageType.Response,
    ProtocolErrorCode.NoError
  )

/** For every other device: it ingests the messages and ignores the ranges. */
export const toBroadcast = (message: Uint8Array, bodyOffset: number) =>
  withHeader(message, bodyOffset, MessageType.Broadcast, null)

export const responseWithoutBody = (
  ownerId: OwnerId,
  errorCode: ProtocolErrorCodeValue
) =>
  createProtocolMessageBuffer(ownerId, {
    messageType: MessageType.Response,
    errorCode,
  }).unwrap()

/**
 * What a round is made of, for the two decisions that need it: whether it
 * carries data worth broadcasting, and whether the sender wants another round.
 *
 * This walks the `[messages][ranges]` body — variable-length counts, delta
 * encoded millis, run-length encoded counters and node ids, length-prefixed
 * changes — without materializing anything. Nothing is decrypted (the relay has
 * no key) and nothing is remembered: the counts die with the round.
 *
 * ponytail: mirrors Evolu's internal encoding, which is not exported and is
 * pinned to protocol version 1. It gives up on anything unexpected and every
 * caller then behaves as if it had not looked, so a format change costs the
 * improvements, never the sync. `wire.test.ts` checks it against Evolu's own
 * encoder, which is what fails loudly when that day comes — and the error
 * carries why it gave up, so a drift in production says so in the log rather
 * than degrading in silence.
 */
export interface RoundShape {
  readonly messageCount: number
  readonly rangeCount: number
}

const createInspectError = defineError("InspectError")<{
  readonly reason: string
}>()
export type InspectError = ReturnType<typeof createInspectError>

interface Cursor {
  readonly bytes: Uint8Array
  offset: number
}

const readByte = (cursor: Cursor): number => {
  const byte = cursor.bytes[cursor.offset]
  if (byte === undefined) throw new Error("Out of bounds")
  cursor.offset += 1
  return byte
}

/** Variable-length quantity, as `decodeNonNegativeInt` writes it. */
const readVarInt = (cursor: Cursor): number => {
  let result = 0
  let shift = 0

  for (let index = 0; index < 8; index++) {
    const byte = readByte(cursor)
    result += (byte & 127) * 2 ** shift
    if ((byte & 128) === 0) return result
    shift += 7
  }

  throw new Error("Unterminated variable-length quantity")
}

const skip = (cursor: Cursor, length: number): void => {
  cursor.offset += length
  if (cursor.offset > cursor.bytes.length) throw new Error("Out of bounds")
}

const skipRunLengthEncoded = (
  cursor: Cursor,
  count: number,
  skipValue: (cursor: Cursor) => void
): void => {
  let index = 0
  while (index < count) {
    skipValue(cursor)
    const runLength = readVarInt(cursor)
    if (runLength === 0 || runLength > count - index) {
      throw new Error("Invalid run length")
    }
    index += runLength
  }
}

const skipTimestamps = (cursor: Cursor, count: number): void => {
  for (let index = 0; index < count; index++) readVarInt(cursor)
  skipRunLengthEncoded(cursor, count, readVarInt)
  skipRunLengthEncoded(cursor, count, (inner) => {
    skip(inner, nodeIdBytesLength)
  })
}

export const inspect = (
  message: Uint8Array,
  bodyOffset: number
): Result<RoundShape, InspectError> =>
  trySync(
    () => {
      const cursor: Cursor = { bytes: message, offset: bodyOffset }

      const messageCount = readVarInt(cursor)
      skipTimestamps(cursor, messageCount)
      for (let index = 0; index < messageCount; index++) {
        skip(cursor, readVarInt(cursor))
      }

      const rangeCount =
        cursor.offset >= message.length ? 0 : readVarInt(cursor)

      return { messageCount, rangeCount }
    },
    (cause) =>
      createInspectError({
        reason: cause instanceof Error ? cause.message : String(cause),
      })
  )
