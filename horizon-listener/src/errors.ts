/**
 * Copyright (c) 2026 stellar-compliance-kit
 * SPDX-License-Identifier: MIT
 */

/**
 * The cursor (or start ledger) we asked for is older than the RPC's retention
 * window, so the node has pruned that history (#813).
 *
 * This is deliberately a typed error and not a generic retryable failure: the
 * listener can retry the same cursor forever and never make progress, because
 * the data it wants no longer exists. Callers must decide between failing loudly
 * and resuming from the oldest ledger the node still has.
 */
export class CursorOutOfRangeError extends Error {
  /** What we asked for: a cursor string, or the start ledger we started from. */
  readonly requested: string | number;
  /** Oldest ledger the node still retains, when it told us. */
  readonly oldestAvailable?: number;

  constructor(
    requested: string | number,
    oldestAvailable?: number,
    options?: { cause?: unknown },
  ) {
    const available =
      oldestAvailable === undefined
        ? 'the node did not report its oldest available ledger'
        : `oldest available ledger is ${oldestAvailable}`;
    super(
      `horizon-listener: requested position ${String(requested)} is outside the RPC retention ` +
        `window (${available}). The history has been pruned, so retrying this position cannot succeed.`,
    );
    this.name = 'CursorOutOfRangeError';
    this.requested = requested;
    this.oldestAvailable = oldestAvailable;
    (this as { cause?: unknown }).cause = options?.cause;
  }
}

/**
 * Recognises the out-of-range shapes Soroban RPC returns.
 *
 * The RPC has no dedicated error code for pruned history, so this matches the
 * messages nodes actually send — a generic retry loop turns them into an infinite
 * spin, which is exactly what #813 is about.
 */
export function isCursorOutOfRangeError(error: unknown): boolean {
  const message = extractMessage(error).toLowerCase();
  if (!message) return false;

  // These phrases are unambiguous on their own — a node only talks about its
  // retention window or pruned history when the requested position is unusable.
  const unambiguous =
    message.includes('retention window') ||
    message.includes('pruned') ||
    message.includes('out of range') ||
    message.includes('outside the ledger range') ||
    message.includes('outside of the ledger range');

  if (unambiguous) return true;

  // A bare "ledger range" mention is only a gap if it also names our position.
  return message.includes('ledger range') && message.includes('ledger');
}

/** Best-effort extraction of the oldest ledger a node reports alongside the error. */
export function extractOldestAvailableLedger(error: unknown): number | undefined {
  const message = extractMessage(error);
  const match = /oldest(?:\s+available)?(?:\s+ledger)?\s*(?:is|:)?\s*(\d{1,12})/i.exec(message);
  if (match) return Number(match[1]);

  const rangeMatch =
    /(?:from|between|range)\s*(\d{1,12})\s*(?:to|-|and|\.\.)\s*\d{1,12}/i.exec(message);
  if (rangeMatch) return Number(rangeMatch[1]);

  return undefined;
}

function extractMessage(error: unknown): string {
  if (typeof error === 'string') return error;
  if (error && typeof error === 'object') {
    const candidate = error as { message?: unknown; response?: { data?: unknown } };
    if (typeof candidate.message === 'string' && candidate.message) return candidate.message;
    const data = candidate.response?.data as { detail?: unknown; message?: unknown } | undefined;
    if (typeof data?.detail === 'string') return data.detail;
    if (typeof data?.message === 'string') return data.message;
  }
  return '';
}
