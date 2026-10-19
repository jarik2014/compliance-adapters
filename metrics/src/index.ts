/**
 * Copyright (c) 2026 stellar-compliance-kit
 * SPDX-License-Identifier: MIT
 */

/**
 * Lightweight, zero-dependency metrics shared across compliance-adapters
 * packages.
 *
 * Design goals
 * ─────────────
 * • Low-cardinality: labels are `phase` and `outcome` only. Consuming
 *   packages supply their own bounded `Phase` union (e.g. horizon-listener's
 *   `'rpc_poll' | 'event_relay' | 'webhook'`) — never ledger IDs, contract
 *   addresses, transaction hashes, or cursor values.
 * • Prometheus-compatible: `MetricsRegistry.expose()` returns a Prometheus
 *   text-format string suitable for scraping from a `/metrics` endpoint.
 * • Optional: `NoopMetricsRegistry` is a zero-overhead default. Operators who
 *   want visibility pass their own `MetricsRegistry` instance; code paths
 *   that receive no registry stay completely side-effect-free.
 */

export type Outcome = 'success' | 'failure' | 'cancelled';

/** A single counter: monotonically increasing integer keyed by phase+outcome. */
export interface Counter<Phase extends string> {
  inc(phase: Phase, outcome: Outcome): void;
  get(phase: Phase, outcome: Outcome): number;
}

/**
 * A gauge: a value that can go up and down, read at scrape time.
 *
 * Counters and histograms in this package are keyed by `phase`; gauges are
 * keyed by name because they describe a state (e.g. how far behind the chain
 * head the listener is), not a call.
 */
export interface Gauge {
  set(name: string, value: number): void;
  get(name: string): number | undefined;
}

/**
 * A histogram that tracks duration samples in configurable buckets (ms).
 * Exposes `count`, `sum`, and per-bucket cumulative counts for Prometheus.
 */
export interface Histogram<Phase extends string> {
  observe(phase: Phase, durationMs: number): void;
  /**
   * Returns `{ count, sum, buckets }` for a given phase.
   * `buckets` maps upper-bound (ms) → cumulative count of observations ≤ that
   * bound. An `+Inf` bucket is appended automatically.
   */
  snapshot(phase: Phase): { count: number; sum: number; buckets: Map<number | '+Inf', number> };
}

// ── Counter implementation ────────────────────────────────────────────────────

class DefaultCounter<Phase extends string> implements Counter<Phase> {
  private readonly data = new Map<string, number>();

  private key(phase: Phase, outcome: Outcome): string {
    return `${phase}:${outcome}`;
  }

  inc(phase: Phase, outcome: Outcome): void {
    const k = this.key(phase, outcome);
    this.data.set(k, (this.data.get(k) ?? 0) + 1);
  }

  get(phase: Phase, outcome: Outcome): number {
    return this.data.get(this.key(phase, outcome)) ?? 0;
  }
}

// ── Histogram implementation ──────────────────────────────────────────────────

/**
 * Default bucket upper-bounds (ms).  They cover sub-millisecond fast paths all
 * the way through 10 s network timeouts typical for RPC calls.
 */
export const DEFAULT_HISTOGRAM_BUCKETS = [5, 25, 100, 250, 500, 1000, 2500, 5000, 10000];

class DefaultHistogram<Phase extends string> implements Histogram<Phase> {
  private readonly buckets: number[];
  // phase → raw observation list
  private readonly observations = new Map<Phase, number[]>();

  constructor(buckets: number[] = DEFAULT_HISTOGRAM_BUCKETS) {
    this.buckets = [...buckets].sort((a, b) => a - b);
  }

  observe(phase: Phase, durationMs: number): void {
    if (!this.observations.has(phase)) {
      this.observations.set(phase, []);
    }
    this.observations.get(phase)!.push(durationMs);
  }

  snapshot(phase: Phase): { count: number; sum: number; buckets: Map<number | '+Inf', number> } {
    const obs = this.observations.get(phase) ?? [];
    const count = obs.length;
    const sum = obs.reduce((acc, v) => acc + v, 0);

    const bucketMap = new Map<number | '+Inf', number>();
    for (const bound of this.buckets) {
      const cumulative = obs.filter((v) => v <= bound).length;
      bucketMap.set(bound, cumulative);
    }
    // +Inf bucket always equals total count (all observations are ≤ infinity)
    bucketMap.set('+Inf', count);

    return { count, sum, buckets: bucketMap };
  }
}

// ── Gauge implementation ─────────────────────────────────────────────────────

class DefaultGauge implements Gauge {
  private readonly values = new Map<string, number>();

  set(name: string, value: number): void {
    this.values.set(name, value);
  }

  get(name: string): number | undefined {
    return this.values.get(name);
  }
}

// ── No-op implementations (disabled metrics) ─────────────────────────────────

class NoopGauge implements Gauge {
  set(_name: string, _value: number): void {}
  get(_name: string): number | undefined {
    return undefined;
  }
}

class NoopCounter<Phase extends string> implements Counter<Phase> {
  inc(_phase: Phase, _outcome: Outcome): void {}
  get(_phase: Phase, _outcome: Outcome): number {
    return 0;
  }
}

class NoopHistogram<Phase extends string> implements Histogram<Phase> {
  observe(_phase: Phase, _durationMs: number): void {}
  snapshot(_phase: Phase): { count: number; sum: number; buckets: Map<number | '+Inf', number> } {
    return { count: 0, sum: 0, buckets: new Map() };
  }
}

// ── Registry ──────────────────────────────────────────────────────────────────

export interface MetricsRegistryOptions<Phase extends string> {
  /** All phase values the registry should expose in `expose()`. */
  phases: Phase[];
  /** Prefix prepended to every metric name. */
  prefix: string;
  /** Custom histogram bucket boundaries in milliseconds. */
  histogramBuckets?: number[];
  /**
   * Gauge names this registry may publish (e.g. `ledger_lag`). Declared up front
   * so `expose()` can emit a HELP/TYPE line per gauge; unset gauges are skipped.
   */
  gauges?: string[];
}

/**
 * Holds a counter and a histogram for a package's phases.
 *
 * Pass an instance to a package's options to enable recording. Omit it
 * entirely to use the built-in no-op registry — there is zero overhead.
 */
export class MetricsRegistry<Phase extends string> {
  readonly counter: Counter<Phase>;
  readonly histogram: Histogram<Phase>;
  readonly gauge: Gauge;
  private readonly prefix: string;
  private readonly phases: Phase[];
  private readonly gaugeNames: string[];

  constructor(options: MetricsRegistryOptions<Phase>) {
    this.prefix = options.prefix;
    this.phases = options.phases;
    this.gaugeNames = options.gauges ?? [];
    this.counter = new DefaultCounter<Phase>();
    this.histogram = new DefaultHistogram<Phase>(options.histogramBuckets);
    this.gauge = new DefaultGauge();
  }

  /**
   * Emit a Prometheus text-format exposition for all tracked metrics.
   *
   * Example output:
   * ```
   * # HELP horizon_listener_requests_total Total requests by phase and outcome
   * # TYPE horizon_listener_requests_total counter
   * horizon_listener_requests_total{phase="rpc_poll",outcome="success"} 42
   * ...
   * # HELP horizon_listener_duration_ms_bucket Histogram of phase durations (ms)
   * # TYPE horizon_listener_duration_ms histogram
   * horizon_listener_duration_ms_bucket{phase="rpc_poll",le="5"} 3
   * ...
   * ```
   */
  expose(): string {
    const lines: string[] = [];

    // ── Counter ──────────────────────────────────────────────────────────────
    const counterName = `${this.prefix}_requests_total`;
    lines.push(`# HELP ${counterName} Total requests by phase and outcome`);
    lines.push(`# TYPE ${counterName} counter`);

    const outcomes: Outcome[] = ['success', 'failure', 'cancelled'];

    for (const phase of this.phases) {
      for (const outcome of outcomes) {
        const v = this.counter.get(phase, outcome);
        lines.push(`${counterName}{phase="${phase}",outcome="${outcome}"} ${v}`);
      }
    }

    // ── Histogram ────────────────────────────────────────────────────────────
    const histName = `${this.prefix}_duration_ms`;
    lines.push(`# HELP ${histName}_bucket Histogram of phase durations in milliseconds`);
    lines.push(`# TYPE ${histName} histogram`);

    for (const phase of this.phases) {
      const { count, sum, buckets } = this.histogram.snapshot(phase);
      for (const [le, cumCount] of buckets) {
        lines.push(`${histName}_bucket{phase="${phase}",le="${String(le)}"} ${cumCount}`);
      }
      lines.push(`${histName}_sum{phase="${phase}"} ${sum}`);
      lines.push(`${histName}_count{phase="${phase}"} ${count}`);
    }

    // ── Gauges ───────────────────────────────────────────────────────────────
    // Only gauges that have been set are exposed: Prometheus would otherwise
    // report a phantom zero for a state the process never measured.
    for (const name of this.gaugeNames ?? []) {
      const value = this.gauge.get(name);
      if (value === undefined) continue;
      const gaugeName = name.startsWith(this.prefix) ? name : `${this.prefix}_${name}`;
      lines.push(`# HELP ${gaugeName} Gauge reported by ${this.prefix}`);
      lines.push(`# TYPE ${gaugeName} gauge`);
      lines.push(`${gaugeName} ${value}`);
    }

    return lines.join('\n') + '\n';
  }
}

// ── Shared no-op singleton ────────────────────────────────────────────────────

/**
 * A no-op registry that discards all observations.
 * Used as the default so callers that don't provide a registry pay no cost.
 */
export class NoopMetricsRegistry<Phase extends string> {
  readonly counter: Counter<Phase> = new NoopCounter<Phase>();
  readonly histogram: Histogram<Phase> = new NoopHistogram<Phase>();
  readonly gauge: Gauge = new NoopGauge();

  expose(): string {
    return '';
  }
}

export type AnyMetricsRegistry<Phase extends string> =
  MetricsRegistry<Phase> | NoopMetricsRegistry<Phase>;
