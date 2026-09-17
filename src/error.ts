import type { Typed, TypeName } from "@evolu/common"

/**
 * The typed-error factory. See AGENTS.md — expected failures return a
 * `Result` whose error carries a literal `type`, and this is what mints one.
 *
 * ```ts
 * const createInspectError = defineError("InspectError")<{
 *   readonly reason: string
 * }>()
 * export type InspectError = ReturnType<typeof createInspectError>
 *
 * createInspectError({ reason: "Out of bounds" })
 * // { type: "InspectError", reason: "Out of bounds" }
 * ```
 */

/**
 * A payload of nothing. This is `type-fest`'s `EmptyObject` written out,
 * because `@evolu/common` is the only dependency here: the unique symbol is
 * what makes the type match no ordinary object, so the "no payload" branch
 * below cannot be reached by accident.
 */
declare const emptyPayload: unique symbol
type EmptyPayload = { [emptyPayload]?: never }

type ErrorFactory<
  TType extends TypeName,
  TShape extends object,
> = keyof TShape extends never
  ? () => Typed<TType>
  : EmptyPayload extends TShape
    ? () => Typed<TType>
    : (shape: TShape) => Typed<TType> & TShape

export const defineError =
  <TType extends TypeName>(type: TType) =>
  <TShape extends object = EmptyPayload>() =>
    ((shape?: TShape) => ({
      type,
      ...(shape ?? {}),
    })) as ErrorFactory<TType, TShape>
