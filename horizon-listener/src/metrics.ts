/**
 * Copyright (c) 2026 stellar-compliance-kit
 * SPDX-License-Identifier: MIT
 */

/**
 * horizon-listener's phase-specific binding of the generic
 * `@compliance-adapters/metrics` registry (shared with sanctions-oracle).
 *
 * Phases tracked
 * ──────────────
 * rpc_poll    — each call to `eventSource.getEvents()`
 * event_relay — each call to the `onEvent` handler (one per received event)
 * webhook     — each call to a `WebhookSender.send()` (tracked at call site)
 */

import {
  MetricsRegistry as GenericMetricsRegistry,
  NoopMetricsRegistry as GenericNoopMetricsRegistry,
  DEFAULT_HISTOGRAM_BUCKETS,
  type Counter,
  type Gauge,
  type Histogram,
  type Outcome,
  type MetricsRegistryOptions as GenericMetricsRegistryOptions,
} from '@compliance-adapters/metrics';

export { DEFAULT_HISTOGRAM_BUCKETS };
export type { Counter, Gauge, Histogram, Outcome };

export type Phase = 'rpc_poll' | 'event_relay' | 'webhook';

const PHASES: Phase[] = ['rpc_poll', 'event_relay', 'webhook'];

export type MetricsRegistryOptions = Omit<
  GenericMetricsRegistryOptions<Phase>,
  'phases' | 'prefix'
> & {
  /** Prefix prepended to every metric name (default: `horizon_listener`). */
  prefix?: string;
};

/**
 * Holds a counter and a histogram for horizon-listener phases.
 *
 * Pass an instance to `HorizonListenerOptions.metrics` (or
 * `HttpWebhookSenderOptions.metrics`) to enable recording.  Omit it entirely
 * to use the built-in no-op registry — there is zero overhead.
 */
export class MetricsRegistry extends GenericMetricsRegistry<Phase> {
  constructor(options: MetricsRegistryOptions = {}) {
    super({
      ...options,
      prefix: options.prefix ?? 'horizon_listener',
      phases: PHASES,
      // Gauges are state, not per-call samples: `ledger_lag` is how far behind
      // the RPC's chain head the last poll was (#812).
      gauges: options.gauges ?? ['ledger_lag'],
    });
  }
}

/**
 * A no-op registry that discards all observations.
 * Used as the default so callers that don't provide a registry pay no cost.
 */
export class NoopMetricsRegistry extends GenericNoopMetricsRegistry<Phase> {}

export type AnyMetricsRegistry = MetricsRegistry | NoopMetricsRegistry;
