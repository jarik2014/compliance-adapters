/**
 * Copyright (c) 2026 stellar-compliance-kit
 * SPDX-License-Identifier: MIT
 */

import type { EventSource, RawContractEvent } from './eventSource';
import { CursorOutOfRangeError, isCursorOutOfRangeError } from './errors';
import { computeBackoffDelayMs, type BackoffOptions } from '@compliance-adapters/backoff';
import { type AnyMetricsRegistry, NoopMetricsRegistry } from './metrics';
import { type AnyTracer, NoopTracer } from './tracing';
import { type Logger, consoleLogger } from '@compliance-adapters/logger';

// Re-export so existing consumers that imported Logger from this module
// continue to compile without changes.
export type { Logger };

export interface HorizonListenerOptions {
  eventSource: EventSource;
  onEvent: (event: RawContractEvent) => Promise<void> | void;
  onEventFailure?: (event: RawContractEvent, error: unknown) => void | Promise<void>;
  /** Retry failed event handlers. `maxRetries` is the maximum number of attempts. */
  eventRetry?: { maxRetries?: number; jitter?: boolean; baseMs?: number; maxMs?: number };
  pollIntervalMs?: number;
  /**
   * Maximum number of consecutive polling failures before giving up.
   * When a poll fails, a counter increments; it resets to 0 on success.
   * Once `attempts >= maxRetries`, the listener throws with an error.
   *
   * @default 10
   *
   * **Important:** `maxRetries: 0` means "fail immediately on the first error"
   * (since 1 >= 0), not "disable the retry limit" or "retry infinitely".
   * To disable the retry ceiling entirely, use `maxRetries: Infinity`.
   *
   * Example: with `maxRetries: 3`, the listener allows up to 3 consecutive
   * failed polls (attempts 1, 2, 3) before throwing; the error message
   * will say "giving up after 3 consecutive failed polls".
   */
  maxRetries?: number;
  logger?: Logger;
  // Injectable so tests can drive time with Jest fake timers instead of waiting
  // on the real wall clock.
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
  // Injectable so tests can force deterministic (or jitter-free) backoff delays
  // instead of depending on Math.random.
  backoffOptions?: BackoffOptions;
  // When set, the listener pages through all historical events from this ledger
  // before entering normal live polling. Each page is consumed immediately
  // (without sleeping pollIntervalMs between pages); the listener only switches
  // to interval-based polling once a page returns zero events.
  startLedger?: number;
  // 'poll' (default): fixed-interval polling, always sleeps pollIntervalMs
  // between each call regardless of whether events were returned.
  // 'stream': polls again immediately when events were returned, reducing
  // latency for high-activity contracts; falls back to pollIntervalMs when
  // a poll returns empty (quiet period).
  mode?: 'poll' | 'stream';
  /**
   * What to do when the RPC says our position is outside its retention window
   * (#813) — the cursor or start ledger points at pruned history.
   *
   * - `'fail'` (default): stop and throw `CursorOutOfRangeError`. Retrying a
   *   position the node no longer has can never succeed, so a generic retry
   *   loop would spin forever.
   * - `'skipToOldest'`: restart from the oldest ledger the node still retains
   *   (requires the source to implement `restart`), accept the gap, and log it.
   */
  onGap?: 'fail' | 'skipToOldest';
  /**
   * Optional metrics registry.  Pass a `MetricsRegistry` instance to record
   * per-phase counters and latency histograms.  When omitted (or when a
   * `NoopMetricsRegistry` is passed) all instrumentation is zero-overhead.
   */
  metrics?: AnyMetricsRegistry;
  /**
   * Optional tracer for OpenTelemetry-compatible distributed tracing.
   * When omitted, a no-op tracer is used — zero overhead and no exports.
   *
   * To enable tracing, pass a `DefaultTracer` configured with an exporter:
   * ```ts
   * import { DefaultTracer } from 'horizon-listener';
   * const tracer = new DefaultTracer({
   *   serviceName: 'horizon-listener',
   *   exporter: async (span) => { await otlpExporter.export(span); },
   * });
   * ```
   */
  tracer?: AnyTracer;
}

// Below this, polling a remote Soroban RPC is almost certainly a misconfiguration
// (e.g. `50` typed instead of `5000`) and risks provider rate-limiting.
export const MIN_RECOMMENDED_POLL_INTERVAL_MS = 250;

const defaultSleep = (ms: number, signal?: AbortSignal): Promise<void> =>
  new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason ?? new Error('Sleep aborted'));
      return;
    }
    const timeoutId = setTimeout(resolve, ms);
    signal?.addEventListener(
      'abort',
      () => {
        clearTimeout(timeoutId);
        reject(signal.reason ?? new Error('Sleep aborted'));
      },
      { once: true },
    );
  });

export class HorizonListener {
  private readonly eventSource: EventSource;
  private readonly onEvent: (event: RawContractEvent) => Promise<void> | void;
  private readonly onEventFailure?: (event: RawContractEvent, error: unknown) => void | Promise<void>;
  private readonly eventRetry?: HorizonListenerOptions['eventRetry'];
  private readonly pollIntervalMs: number;
  private readonly maxRetries: number;
  private readonly logger: Logger;
  private readonly sleep: (ms: number, signal?: AbortSignal) => Promise<void>;
  private readonly backoffOptions: BackoffOptions;
  private readonly mode: 'poll' | 'stream';
  private readonly onGap: 'fail' | 'skipToOldest';
  private readonly metrics: AnyMetricsRegistry;
  private readonly tracer: AnyTracer;

  private cursor: string | undefined;
  /** Highest ledger we have successfully relayed; basis for the ledger-lag gauge. */
  private processedLedger: number | undefined;
  private running = false;
  private attempt = 0;
  private backfilling = false;
  private sleepAbortController: AbortController | undefined;

  constructor(options: HorizonListenerOptions) {
    this.eventSource = options.eventSource;
    this.onEvent = options.onEvent;
    this.onEventFailure = options.onEventFailure;
    this.eventRetry = options.eventRetry;
    this.pollIntervalMs = options.pollIntervalMs ?? 5000;
    this.maxRetries = options.maxRetries ?? 10;
    this.logger = options.logger ?? consoleLogger;
    if (this.pollIntervalMs < MIN_RECOMMENDED_POLL_INTERVAL_MS) {
      this.logger.warn(
        `horizon-listener: pollIntervalMs is ${this.pollIntervalMs}ms, below the recommended ` +
          `minimum of ${MIN_RECOMMENDED_POLL_INTERVAL_MS}ms; this may hammer the Soroban RPC ` +
          'endpoint and get throttled. Ignore this warning for local low-latency testing.',
      );
    }
    this.sleep = options.sleep ?? defaultSleep;
    this.backoffOptions = options.backoffOptions ?? {};
    this.backfilling = options.startLedger != null;
    this.mode = options.mode ?? 'poll';
    this.onGap = options.onGap ?? 'fail';
    this.metrics = options.metrics ?? new NoopMetricsRegistry();
    this.tracer = options.tracer ?? new NoopTracer();
  }

  // Soroban RPC's getEvents is a polling/cursor API, not a persistent stream, so
  // "reconnecting" here just means: pause, then poll again with backoff.
  /**
   * Consumes events as an async iterator (#815).
   *
   * Callback-style `onEvent` does not compose with modern stream processing:
   * `for await`, pipelines and `AbortSignal`. This method pulls on demand —
   * a page is fetched only when the consumer asks for the next event, and the
   * cursor advances only once a page is fully consumed — so a slow consumer
   * applies backpressure instead of building an unbounded buffer.
   *
   * ```ts
   * for await (const event of listener.events({ signal })) {
   *   await handle(event);           // slow consumer → no extra polling
   * }
   * ```
   *
   * Breaking out of the loop (or aborting the signal) ends it cleanly: no
   * further polls happen and the iterator returns. Do not run this alongside
   * `start()` on the same instance — they would both advance the cursor.
   */
  async *events(options: { signal?: AbortSignal } = {}): AsyncIterableIterator<RawContractEvent> {
    const signal = options.signal;
    let cursor = this.cursor;
    let page: { events: RawContractEvent[]; nextCursor: string; latestLedger?: number } | null = null;
    let index = 0;

    while (!signal?.aborted) {
      if (page === null) {
        page = await this.eventSource.getEvents(cursor);
        index = 0;

        if (page.events.length === 0) {
          // Quiet period: hand the cursor over and wait before asking again.
          cursor = page.nextCursor;
          page = null;
          await this.sleep(this.pollIntervalMs, signal);
          continue;
        }
      }

      if (index >= page.events.length) {
        // Page fully consumed — only now is it safe to move past it.
        cursor = page.nextCursor;
        page = null;
        continue;
      }

      const event = page.events[index];
      index += 1;

      // Account for the event before handing it over: the consumer receives a
      // fully-delivered event, so lag never shows a value the consumer would
      // contradict. Same rule as the callback path — only relayed ledgers count.
      this.processedLedger = event.ledger;
      if (typeof page.latestLedger === 'number') {
        const lag = Math.max(0, page.latestLedger - this.processedLedger);
        this.metrics.gauge.set('ledger_lag', lag);
      }

      yield event;
    }

    this.logger.debug('horizon-listener: events() iterator finished');
  }

  /**
   * Starts the event listener loop.
   * @throws {Error} When max retries are exceeded for consecutive polling failures.
   * This is an unrecoverable error that indicates the listener cannot continue.
   */
  async start(): Promise<void> {
    this.running = true;
    this.attempt = 0;

    while (this.running) {
      // ── rpc_poll span ────────────────────────────────────────────────────
      const pollSpan = this.tracer.startSpan('rpc_poll');
      pollSpan.setAttribute('poll.attempt', this.attempt);

      let response: { events: RawContractEvent[]; nextCursor: string; latestLedger?: number };
      const pollStart = Date.now();
      try {
        response = await this.eventSource.getEvents(this.cursor);
      } catch (err) {
        const pollDuration = Date.now() - pollStart;
        this.metrics.counter.inc('rpc_poll', 'failure');
        this.metrics.histogram.observe('rpc_poll', pollDuration);

        // ── pruned history (#813) ────────────────────────────────────────────
        // Not a transient failure: the position itself no longer exists, so the
        // backoff-and-retry path below would spin until maxRetries for nothing.
        if (err instanceof CursorOutOfRangeError || isCursorOutOfRangeError(err)) {
          const gapError =
            err instanceof CursorOutOfRangeError
              ? err
              : new CursorOutOfRangeError(this.cursor ?? 'unknown', undefined, { cause: err });

          pollSpan.end('error', gapError);

          if (this.onGap === 'skipToOldest' && typeof this.eventSource.restart === 'function') {
            const resumeFrom = gapError.oldestAvailable;
            if (typeof resumeFrom === 'number') {
              this.logger.warn(
                `horizon-listener: position ${String(gapError.requested)} is outside the RPC ` +
                  `retention window; skipping to the oldest available ledger ${resumeFrom}. ` +
                  'Events before that ledger will never be delivered.',
              );
              this.cursor = undefined;
              this.eventSource.restart(resumeFrom);
              this.attempt = 0;
              continue;
            }

            this.logger.error(
              'horizon-listener: cursor is out of range but the node did not report its oldest ' +
                'available ledger, so onGap="skipToOldest" cannot resume.',
            );
          }

          this.running = false;
          this.metrics.counter.inc('rpc_poll', 'cancelled');
          throw gapError;
        }

        this.attempt += 1;
        pollSpan.setAttribute('poll.attempt', this.attempt);
        pollSpan.end('error', err instanceof Error ? err : new Error(String(err)));

        this.logger.warn(
          `horizon-listener: poll failed (attempt ${this.attempt}/${this.maxRetries}), backing off`,
          err,
        );

        if (this.attempt >= this.maxRetries) {
          this.running = false;
          this.metrics.counter.inc('rpc_poll', 'cancelled');
          // Mark the final poll as cancelled (we gave up, not a transient error)
          this.tracer.startSpan('rpc_poll').end('cancelled');
          throw new Error(
            `horizon-listener: giving up after ${this.attempt} consecutive failed polls`,
          );
        }

        // `attempt` is one-based for retry limits, while backoff uses a zero-based exponent.
        const delayMs = computeBackoffDelayMs(this.attempt - 1, this.backoffOptions);
        this.sleepAbortController = new AbortController();
        try {
          await this.sleep(delayMs, this.sleepAbortController.signal);
        } catch (sleepErr) {
          if (this.sleepAbortController.signal.aborted) {
            break;
          }
          throw sleepErr;
        } finally {
          this.sleepAbortController = undefined;
        }
        continue;
      }

      const pollDuration = Date.now() - pollStart;
      this.metrics.counter.inc('rpc_poll', 'success');
      this.metrics.histogram.observe('rpc_poll', pollDuration);
      pollSpan.setAttribute('poll.event_count', response.events.length);
      pollSpan.end('ok');

      this.attempt = 0;

      for (const event of response.events) {
        const relayStart = Date.now();
        // ── event_relay span ───────────────────────────────────────────────
        // Parent is the poll span so spans form a coherent tree:
        // rpc_poll → event_relay (one child per event)
        const relayContext = { traceId: pollSpan.traceId, spanId: pollSpan.spanId };
        const relaySpan = this.tracer.startSpan('event_relay', relayContext);
        // Low-cardinality attributes only — event ID and contract ID are stable
        // identifiers, not user-data payloads. The event value is redacted by
        // the tracer (redactPayload: true by default).
        relaySpan.setAttribute('event.id', event.id);
        relaySpan.setAttribute('event.contract_id', event.contractId);
        relaySpan.setAttribute('event.ledger', event.ledger);

        this.logger.info('horizon-listener: received contract event', event);
        const maxEventAttempts = this.eventRetry
          ? Math.max(1, this.eventRetry.maxRetries ?? 3)
          : 1;
        let eventError: unknown;
        let eventSucceeded = false;

        for (let eventAttempt = 1; eventAttempt <= maxEventAttempts; eventAttempt += 1) {
          try {
            await this.onEvent(event);
            eventSucceeded = true;
            break;
          } catch (err) {
            eventError = err;
            if (eventAttempt >= maxEventAttempts) {
              break;
            }

            const delayMs = computeBackoffDelayMs(eventAttempt, this.eventRetry);
            this.logger.warn(
              `horizon-listener: onEvent failed (attempt ${eventAttempt}/${maxEventAttempts}), retrying in ${delayMs}ms`,
              err,
            );
            await this.sleep(delayMs);
          }
        }

        if (eventSucceeded) {
          const relayDuration = Date.now() - relayStart;
          this.metrics.counter.inc('event_relay', 'success');
          this.metrics.histogram.observe('event_relay', relayDuration);
          // Only successfully relayed ledgers count as progress; a failed event
          // must keep the lag honest.
          this.processedLedger = event.ledger;
          relaySpan.end('ok');
        } else {
          const relayDuration = Date.now() - relayStart;
          this.logger.error('horizon-listener: onEvent handler threw', eventError);
          this.metrics.counter.inc('event_relay', 'failure');
          this.metrics.histogram.observe('event_relay', relayDuration);
          relaySpan.end(
            'error',
            eventError instanceof Error ? eventError : new Error(String(eventError)),
          );
          if (this.onEventFailure) {
            try {
              await this.onEventFailure(event, eventError);
            } catch (failureErr) {
              this.logger.error('horizon-listener: onEventFailure handler threw', failureErr);
            }
          }
        }
      }

      // ── ledger lag (#812) ─────────────────────────────────────────────────
      // Measured after the batch is relayed, so the number answers "how far
      // behind the chain head are we now" rather than one poll late. Events we
      // have not seen yet are compliance gaps, which makes this the pipeline's
      // main SLO signal. Sources without `latestLedger` leave the gauge alone.
      if (typeof response.latestLedger === 'number') {
        const processed = this.processedLedger;
        const lag =
          typeof processed === 'number' ? Math.max(0, response.latestLedger - processed) : 0;
        this.metrics.gauge.set('ledger_lag', lag);
      }

      this.cursor = response.nextCursor;
      this.logger.debug('horizon-listener: cursor advanced', this.cursor);

      if (this.backfilling) {
        if (response.events.length === 0) {
          this.backfilling = false;
          this.logger.info('horizon-listener: backfill complete, switching to live polling');
        } else {
          this.logger.debug(
            `horizon-listener: backfill page consumed (${response.events.length} events), fetching next page`,
          );
          if (!this.running) {
            break;
          }
          continue;
        }
      }

      if (!this.running) {
        break;
      }

      // In stream mode, skip sleeping when events were returned so the next
      // page is fetched immediately; only sleep during quiet periods.
      if (this.mode === 'stream' && response.events.length > 0) {
        continue;
      }

      this.sleepAbortController = new AbortController();
      try {
        await this.sleep(this.pollIntervalMs, this.sleepAbortController.signal);
      } catch (sleepErr) {
        if (this.sleepAbortController.signal.aborted) {
          break;
        }
        throw sleepErr;
      } finally {
        this.sleepAbortController = undefined;
      }
    }
  }

  stop(): void {
    this.running = false;
    this.sleepAbortController?.abort();
  }

  /**
   * Returns a snapshot of the listener's current health/progress state, for
   * use in liveness/readiness probes or status dashboards.
   */
  getStatus(): {
    running: boolean;
    cursor: string | undefined;
    consecutiveFailures: number;
    backfilling: boolean;
  } {
    return {
      running: this.running,
      cursor: this.cursor,
      consecutiveFailures: this.attempt,
      backfilling: this.backfilling,
    };
  }
}
