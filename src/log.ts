import {
  type Console,
  createConsole,
  createConsoleFormatter,
} from "@evolu/common"

/**
 * The relay's console, built once at a host's composition root.
 *
 * Evolu's default plus the one thing it leaves to the caller: a formatter.
 * Without one the native output writes an entry's args and drops its path, so
 * the `child(ownerId)` the relay logs through would be invisible and every
 * line would have to carry the owner itself.
 */
export const createRelayConsole = (): Console =>
  createConsole({ formatter: createConsoleFormatter()() })
