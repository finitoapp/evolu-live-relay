import { Millis, testAppOwner } from "@evolu/common"
import {
  createProtocolMessageBuffer,
  createTimestamp,
  createTimestampsBuffer,
  type EncryptedDbChange,
  InfiniteUpperBound,
  MessageType,
  RangeType,
  timestampToTimestampBytes,
} from "@evolu/common/local-first"
import { expect, test } from "vitest"

import { inspect, requestBodyOffset } from "./wire.ts"

/**
 * `inspect()` against Evolu's own encoder. It walks an encoding Evolu does not
 * export, so this is the alarm for the day that format moves.
 */

const buildRound = (messages: number, ranges: number) => {
  const buffer = createProtocolMessageBuffer(testAppOwner.id, {
    messageType: MessageType.Request,
    writeKey: testAppOwner.writeKey,
  })

  for (let index = 0; index < messages; index++) {
    buffer.addMessage({
      timestamp: createTimestamp({ millis: Millis.orThrow(index + 1) }),
      change: new Uint8Array(3 + index).fill(7) as EncryptedDbChange,
    })
  }

  for (let index = 0; index < ranges; index++) {
    const timestamps = createTimestampsBuffer()
    if (index > 0) {
      timestamps.add(createTimestamp({ millis: Millis.orThrow(5) }))
    }
    buffer.addRange({
      type: RangeType.Timestamps,
      upperBound:
        index === ranges - 1
          ? InfiniteUpperBound
          : timestampToTimestampBytes(
              createTimestamp({ millis: Millis.orThrow(100 * (index + 1)) })
            ),
      timestamps,
    })
  }

  return buffer.unwrap()
}

test("inspect counts what Evolu's own encoder wrote", () => {
  for (const messages of [0, 1, 3]) {
    for (const ranges of [0, 1, 2]) {
      const round = buildRound(messages, ranges)
      expect({
        messages,
        ranges,
        shape: inspect(round, requestBodyOffset(round)),
      }).toEqual({
        messages,
        ranges,
        shape: {
          ok: true,
          value: { messageCount: messages, rangeCount: ranges },
        },
      })
    }
  }
})

test("inspect gives up instead of guessing, and says why", () => {
  const round = buildRound(2, 1)

  // Truncated mid-body: the walker runs out of bytes.
  const truncated = inspect(
    round.subarray(0, round.length - 4),
    requestBodyOffset(round)
  )
  expect(truncated.ok).toBe(false)
  // The reason is what makes a drift visible in a log rather than silent.
  expect(truncated.ok ? undefined : truncated.error).toEqual({
    type: "InspectError",
    reason: "Out of bounds",
  })

  // Not a body at all.
  expect(inspect(new Uint8Array([255, 255, 255, 255]), 0).ok).toBe(false)
})
