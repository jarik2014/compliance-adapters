# horizon-listener

An example/reference service that polls Soroban RPC for contract events emitted
by the `denylist-gate` and `allowlist-token` contracts, logs them, and
re-emits them to a webhook. Soroban RPC's `getEvents` is a cursor-based polling
API — there is no persistent server-sent-events stream for contract events at
that layer — so this package polls on an interval and tracks a cursor between
calls rather than holding open a live connection. "Reconnect" here means the
polling loop hit an error (RPC unreachable, rate-limited, cursor expired) and
backed off before retrying, not a literal TCP reconnect. This package
demonstrates the integration pattern app developers would build on top of;
it is not itself a production event pipeline.

## Runtime compatibility

This package requires **Node.js 20 or newer** because its HTTP webhook sender
uses the native `fetch` API provided by Node 20. Older Node.js versions do not
provide the required runtime fetch support and are not supported.

## Install

```sh
npm install
```

(This package is part of the `compliance-adapters` npm workspace; run install
from the repo root.)

## Configuration

Copy the root [`.env.example`](../.env.example) to `.env` at the repo root and
fill in the `horizon-listener` variables before running the service:

| Variable | Description |
|---|---|
| `STELLAR_RPC_URL` | Soroban RPC endpoint to poll for contract events |
| `STELLAR_NETWORK_PASSPHRASE` | Must match the network the RPC endpoint serves |
| `DENYLIST_GATE_CONTRACT_ID` | Deployed `denylist-gate` contract to subscribe to |
| `ALLOWLIST_TOKEN_CONTRACT_ID` | Deployed `allowlist-token` contract to subscribe to |
| `WEBHOOK_URL` | Endpoint that receives POSTed contract events |
| `POLL_INTERVAL_MS` | Polling interval in milliseconds (default `5000`) |
| `MAX_RETRIES` | Consecutive failures before the listener gives up (default `10`) |
| `START_LEDGER` | Starting ledger for the very first cursor-less event query (required; must be within the RPC node's retention window) |

See the comments in `.env.example` for allowed values and testnet guidance.

## Quick start: `createWebhookForwarder`

For the common case � forward every contract event to a webhook � use the
`createWebhookForwarder` factory, which wires `RpcEventSource`,
`HttpWebhookSender`, and `HorizonListener` together in one call:

```ts
import { Networks } from '@stellar/stellar-sdk';
import { createWebhookForwarder } from 'horizon-listener';

const listener = createWebhookForwarder({
  eventSource: {
    rpcUrl: 'https://soroban-testnet.stellar.org',
    networkPassphrase: Networks.TESTNET,
    contractIds: [process.env.DENYLIST_GATE_CONTRACT_ID!, process.env.ALLOWLIST_TOKEN_CONTRACT_ID!],
    // Required for the first cursor-less query: a recent ledger within the
    // RPC node's event retention window.
    startLedger: Number(process.env.START_LEDGER),
  },
  webhook: {
    url: 'http://localhost:4000/webhook',
    signingSecret: process.env.WEBHOOK_SIGNING_SECRET,
  },
  listenerOptions: {
    pollIntervalMs: 5000,
  },
});

listener.start().catch((err) => {
  console.error('horizon-listener gave up after repeated failures', err);
  process.exit(1);
});
```

`listenerOptions` accepts every `HorizonListenerOptions` field except
`eventSource` and `onEvent` (e.g. `mode`, `maxRetries`, `logger`,
`onEventFailure`, `metrics`, `tracer`).

## Manual wiring

If you need more control (a custom `EventSource`, extra processing in
`onEvent`, multiple sinks), wire the pieces together yourself:

```ts
import { Networks } from '@stellar/stellar-sdk';
import { RpcEventSource, HorizonListener, HttpWebhookSender } from 'horizon-listener';

const eventSource = new RpcEventSource({
  rpcUrl: 'https://soroban-testnet.stellar.org',
  networkPassphrase: Networks.TESTNET,
  contractIds: [process.env.DENYLIST_GATE_CONTRACT_ID!, process.env.ALLOWLIST_TOKEN_CONTRACT_ID!],
  startLedger: Number(process.env.START_LEDGER),
});

const webhook = new HttpWebhookSender({
  url: 'http://localhost:4000/webhook',
  signingSecret: process.env.WEBHOOK_SIGNING_SECRET,
});

const listener = new HorizonListener({
  eventSource,
  pollIntervalMs: 5000,
  onEvent: async (event) => {
    await webhook.send(event);
  },
});

listener.start().catch((err) => {
  console.error('horizon-listener gave up after repeated failures', err);
  process.exit(1);
});
```

A minimal stub receiver to point `HttpWebhookSender` at during local
development (using [Express](https://expressjs.com/), not a dependency of this
package):

```ts
import express from 'express';

const app = express();
app.use(express.json());
app.post('/webhook', (req, res) => {
  console.log('received event', req.body);
  res.sendStatus(200);
});
app.listen(4000);
```

## Polling mode vs stream mode

`HorizonListener` supports two polling modes via the `mode` option:

- **`poll`** (default) — fixed-interval polling. The listener always waits
  `pollIntervalMs` between each `getEvents` call, regardless of whether events
  were returned. Suitable for most use cases.

- **`stream`** — backoff-only "stream-like" mode. The listener polls again
  immediately after processing a full page of events, and only sleeps
  `pollIntervalMs` when a poll returns no new events. This reduces latency for
  high-activity contracts without hammering the RPC during quiet periods.

```ts
const listener = new HorizonListener({
  eventSource,
  onEvent: async (event) => { /* ... */ },
  mode: 'stream',      // or 'poll' (default)
  pollIntervalMs: 5000, // sleep duration during quiet periods
});
```

`pollIntervalMs` is not hard-limited, but values below 250ms log a warning at
construction time: polling a remote RPC that aggressively is usually a typo
(e.g. `50` instead of `5000`) and risks being throttled by the provider. Very
low intervals remain allowed for local low-latency testing.

## Backfill mode (`startLedger`)

Two different `startLedger` options exist:

- `RpcEventSourceOptions.startLedger` tells Soroban RPC where the very first
  cursor-less `getEvents` query begins. It is **required**: real RPC nodes only
  retain events for a limited recent window (commonly ~24h / 17,280 ledgers),
  so there is no safe default. If it is omitted, the first cursor-less call
  throws a descriptive `horizon-listener` error instead of sending ledger `0`.
- `HorizonListenerOptions.startLedger` switches the listener into **backfill
  mode** on startup, for catching up after downtime.

In backfill mode the listener pages through historical events as fast as the
RPC returns them:

1. Each page is processed (every event goes through `onEvent`) and the cursor
   advances.
2. If the page contained events, the next page is fetched **immediately** �
   the listener does **not** sleep `pollIntervalMs` between backfill pages.
3. The first page that returns **zero events** ends backfill. The listener logs
   `backfill complete, switching to live polling` and from then on behaves
   according to `mode`.

`getStatus().backfilling` reports whether the listener is still catching up.

Interaction with `mode`:

- **`poll`** � after backfill, the listener sleeps `pollIntervalMs` after
  every poll, as usual.
- **`stream`** � during backfill the behaviour is identical (page immediately
  while events are returned). After backfill, `stream` keeps fetching
  immediately whenever a poll returns events and only sleeps during quiet
  periods, so the transition is effectively seamless.

During catch-up, expect a burst of back-to-back RPC calls proportional to the
number of historical pages; size `startLedger` (and your RPC provider's rate
limits) accordingly. Poll failures during backfill use the same exponential
backoff and `maxRetries` handling described below.

## Webhook signature verification

### Custom headers

Receivers behind an API gateway often need a static header (an API key, a tenant
id). Pass it once:

```ts
const webhook = new HttpWebhookSender({
  url: 'https://gateway.example.com/compliance',
  signingSecret: process.env.WEBHOOK_SIGNING_SECRET,
  headers: { 'X-Api-Key': process.env.GATEWAY_API_KEY! },
});
```

The map is merged **under** the reserved headers: `Content-Type`,
`Idempotency-Key`, `X-Timestamp` and `X-Signature` always win, so a custom entry
can never break signing or idempotency.

When `HttpWebhookSender` is constructed with a `signingSecret`, every outbound
request carries an `X-Timestamp` header and an `X-Signature` header of the
form:

```
X-Timestamp: <unix seconds at send time>
X-Signature: sha256=<hex-encoded HMAC-SHA256 of "<X-Timestamp>.<raw request body>", keyed by signingSecret>
```

The timestamp is folded into the signed material specifically so that a
receiver can detect replayed requests: because contract event payloads are
deterministic, a captured request with a valid signature could otherwise be
replayed indefinitely. The HMAC is computed over
`` `${timestamp}.${body}` `` where `body` is the exact JSON string sent as
the request body (before any parsing), so a receiver must verify against the
raw bytes, not a re-serialized version of the parsed body — re-serializing
can change key order or whitespace and produce a different digest.

A receiver must do two things to validate a request: recompute the HMAC over
`<X-Timestamp>.<raw body>` and compare it to `X-Signature` using a
constant-time comparison (to avoid leaking the secret through timing
differences), **and** reject requests whose `X-Timestamp` falls outside a
freshness window — a receiver should reject the request if they don't match,
if either header is missing, or if the timestamp is stale:

```ts
import { createHmac, timingSafeEqual } from 'node:crypto';
import express from 'express';

const WEBHOOK_SIGNING_SECRET = process.env.WEBHOOK_SIGNING_SECRET!;
// Reasonable default: tolerates delivery latency and retry backoff while
// still bounding how long a captured request stays replayable.
const MAX_TIMESTAMP_SKEW_SECONDS = 5 * 60;

const app = express();

// Capture the raw body bytes for HMAC verification before JSON parsing.
app.use(express.json({ verify: (req, _res, buf) => {
  (req as express.Request & { rawBody?: Buffer }).rawBody = buf;
} }));

function isFreshTimestamp(timestampHeader: string | undefined): boolean {
  if (!timestampHeader || !/^\d+$/.test(timestampHeader)) return false;

  const timestamp = Number(timestampHeader);
  const skewSeconds = Math.abs(Date.now() / 1000 - timestamp);
  return skewSeconds <= MAX_TIMESTAMP_SKEW_SECONDS;
}

function isValidSignature(
  rawBody: Buffer,
  timestampHeader: string | undefined,
  signatureHeader: string | undefined,
): boolean {
  if (!timestampHeader || !signatureHeader) return false;

  const signedMaterial = Buffer.concat([Buffer.from(`${timestampHeader}.`), rawBody]);
  const expected = createHmac('sha256', WEBHOOK_SIGNING_SECRET).update(signedMaterial).digest('hex');
  const expectedHeader = `sha256=${expected}`;

  const a = Buffer.from(signatureHeader);
  const b = Buffer.from(expectedHeader);
  return a.length === b.length && timingSafeEqual(a, b);
}

app.post('/webhook', (req, res) => {
  const rawBody = (req as express.Request & { rawBody?: Buffer }).rawBody ?? Buffer.alloc(0);
  const timestampHeader = req.header('X-Timestamp');

  if (
    !isFreshTimestamp(timestampHeader) ||
    !isValidSignature(rawBody, timestampHeader, req.header('X-Signature'))
  ) {
    return res.sendStatus(401);
  }

  console.log('received event', req.body);
  res.sendStatus(200);
});
app.listen(4000);
```

If `signingSecret` is omitted, `HttpWebhookSender` sends requests without
`X-Timestamp` or `X-Signature` headers, exactly as before this feature was
added.

## Reconnect / backoff behavior

If `eventSource.getEvents(...)` throws (RPC unreachable, rate-limited, cursor
expired, etc.), `HorizonListener` does not crash: it logs a warning, waits an
exponentially increasing backoff delay (see `computeBackoffDelayMs` in the
[`@compliance-adapters/backoff`](../backoff/src/index.ts) package, re-exported
from this package's entry point, capped at 30s by default with jitter), and
retries. The retry counter resets to zero after any subsequent successful poll.
If `maxRetries` consecutive failures are exceeded (default 10), `start()`
rejects so the caller knows the listener gave up — in a real deployment a
process manager would be responsible for restarting the process.

Individual `onEvent` handler errors are caught and logged without stopping
the polling loop, so one bad event doesn't take down the whole listener.

## Graceful shutdown

For clean shutdown in a long-running Node process, call `HorizonListener.stop()`
from a signal handler:

```ts
process.on('SIGINT', () => {
  console.log('Shutting down gracefully...');
  listener.stop();
});

process.on('SIGTERM', () => {
  console.log('Shutting down gracefully...');
  listener.stop();
});

listener.start().catch((err) => {
  console.error('horizon-listener gave up after repeated failures', err);
  process.exit(1);
});
```

See `examples/graceful-shutdown.ts` for a complete example using the webhook
forwarder factory.

## Logging

All logging is routed through an injectable `Logger` interface with a default
console-backed implementation. Log levels are used semantically:

- **debug** — Cursor advancement and other low-level operational details
- **info** — Successful event reception (one log per event received)
- **warn** — Recoverable poll failures with automatic retry (e.g., RPC unreachable, rate-limited)
- **error** — Handler errors and critical listener failures

Pass a custom `Logger` instance to `HorizonListener` options to customize
output (e.g., to route to a production logging service).

## Metrics

`horizon-listener` exposes a Prometheus-compatible metrics registry that can be
attached to both the listener and the webhook sender. This is useful when a
long-lived process needs visibility into poll health, relay latency, and
webhook delivery outcomes without instrumenting each call site by hand.

### Enabling metrics

```ts
import {
  HorizonListener,
  HttpWebhookSender,
  MetricsRegistry,
} from 'horizon-listener';

const metrics = new MetricsRegistry();

const webhook = new HttpWebhookSender({
  url: 'http://localhost:4000/webhook',
  metrics,
});

const listener = new HorizonListener({
  eventSource,
  onEvent: async (event) => {
    await webhook.send(event);
  },
  metrics,
});
```

Both `HorizonListenerOptions.metrics` and `HttpWebhookSenderOptions.metrics`
accept a `MetricsRegistry` instance (or `NoopMetricsRegistry` for zero-overhead
behavior when metrics are disabled). If you omit the `metrics` option,
`horizon-listener` falls back to a no-op registry and records nothing.

### Tracked phases and outcomes

The registry records per-phase counters and duration histograms for these
phases:

- `rpc_poll` — each polling call to `eventSource.getEvents()`
- `event_relay` — each call to the `onEvent` callback
- `webhook` — each outbound `HttpWebhookSender.send()` attempt

Each phase carries a low-cardinality `outcome` label:

- `success` — the phase completed successfully
- `failure` — the phase returned an error or failed a request
- `cancelled` — a poll was abandoned because the listener hit `maxRetries`

The exported metric names are prefixed with the value from `prefix` (default:
`horizon_listener`):

- `${prefix}_requests_total{phase="...",outcome="..."}`
- `${prefix}_duration_ms_bucket{phase="...",le="..."}`
- `${prefix}_duration_ms_sum{phase="..."}`
- `${prefix}_duration_ms_count{phase="..."}`

### Scraping example

```ts
const metrics = new MetricsRegistry({ prefix: 'horizon_listener' });
console.log(metrics.expose());
```

Example output:

```text
# HELP horizon_listener_requests_total Total requests by phase and outcome
# TYPE horizon_listener_requests_total counter
horizon_listener_requests_total{phase="rpc_poll",outcome="success"} 42
horizon_listener_requests_total{phase="rpc_poll",outcome="failure"} 3
horizon_listener_requests_total{phase="event_relay",outcome="success"} 37
horizon_listener_requests_total{phase="event_relay",outcome="failure"} 2
horizon_listener_requests_total{phase="webhook",outcome="success"} 32
horizon_listener_requests_total{phase="webhook",outcome="failure"} 5
horizon_listener_requests_total{phase="webhook",outcome="cancelled"} 0

# HELP horizon_listener_duration_ms_bucket Histogram of phase durations in milliseconds
# TYPE horizon_listener_duration_ms histogram
horizon_listener_duration_ms_bucket{phase="rpc_poll",le="5"} 10
horizon_listener_duration_ms_bucket{phase="rpc_poll",le="25"} 25
horizon_listener_duration_ms_bucket{phase="rpc_poll",le="100"} 39
horizon_listener_duration_ms_bucket{phase="rpc_poll",le="+Inf"} 42
horizon_listener_duration_ms_sum{phase="rpc_poll"} 1245
horizon_listener_duration_ms_count{phase="rpc_poll"} 42
```

This output is intentionally Prometheus-compatible, so it can be scraped by a
standard `/metrics` endpoint or by a sidecar collector in a long-running
operator environment.

## Links

- [Root README](../README.md)
- [`compliance-primitives`](https://github.com/stellar-compliance-kit/compliance-primitives) —
  the on-chain `denylist-gate` and `allowlist-token` Soroban contracts this
  package listens to events from.
