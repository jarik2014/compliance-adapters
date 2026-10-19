import { MetricsRegistry, NoopMetricsRegistry } from '../src/metrics';
import { HorizonListener } from '../src/listener';
import type { EventSource, RawContractEvent } from '../src/eventSource';

function makeEvent(overrides: Partial<RawContractEvent> = {}): RawContractEvent {
  return {
    id: 'evt-1',
    contractId: 'CDENYLISTGATE',
    ledger: 100,
    topic: ['denylist_added'],
    value: {},
    ...overrides,
  };
}

type Poll = { events: RawContractEvent[]; nextCursor?: string; latestLedger?: number };

/**
 * Runs the listener over a scripted list of polls and stops it after the last
 * one — the same driving pattern the other listener tests use.
 */
async function drive(
  polls: Poll[],
  registry: MetricsRegistry | NoopMetricsRegistry,
): Promise<void> {
  const getEvents = jest.fn();
  for (const poll of polls) {
    getEvents.mockResolvedValueOnce({
      events: poll.events,
      nextCursor: poll.nextCursor ?? 'cursor',
      ...(typeof poll.latestLedger === 'number' ? { latestLedger: poll.latestLedger } : {}),
    });
  }

  const lastEventId = polls[polls.length - 1].events.slice(-1)[0]?.id;
  const listener: HorizonListener = new HorizonListener({
    eventSource: { getEvents } as EventSource,
    onEvent: (event) => {
      if (event.id === lastEventId) {
        listener.stop();
      }
    },
    pollIntervalMs: 1000,
    sleep: async () => {},
    metrics: registry,
  });

  await listener.start();
  expect(getEvents).toHaveBeenCalledTimes(polls.length);
}

describe('ledger lag gauge (#812)', () => {
  it('reports how far behind the chain head the listener is', async () => {
    const registry = new MetricsRegistry();

    await drive([{ events: [makeEvent({ ledger: 100 })], latestLedger: 140 }], registry);

    expect(registry.gauge.get('ledger_lag')).toBe(40);
  });

  it('updates the gauge on every poll', async () => {
    const registry = new MetricsRegistry();

    await drive(
      [
        { events: [makeEvent({ id: 'evt-1', ledger: 100 })], latestLedger: 110 },
        { events: [makeEvent({ id: 'evt-2', ledger: 120 })], latestLedger: 130 },
      ],
      registry,
    );

    // Second poll: processed 120, chain head 130.
    expect(registry.gauge.get('ledger_lag')).toBe(10);
  });

  it('never reports a negative lag when events run ahead of the reported head', async () => {
    const registry = new MetricsRegistry();

    await drive([{ events: [makeEvent({ ledger: 500 })], latestLedger: 490 }], registry);

    expect(registry.gauge.get('ledger_lag')).toBe(0);
  });

  it('leaves the gauge unset when the source does not report a chain head', async () => {
    const registry = new MetricsRegistry();

    await drive([{ events: [makeEvent({ ledger: 100 })] }], registry);

    expect(registry.gauge.get('ledger_lag')).toBeUndefined();
  });

  it('exposes the gauge in Prometheus format under the package prefix', async () => {
    const registry = new MetricsRegistry();
    await drive([{ events: [makeEvent({ ledger: 100 })], latestLedger: 120 }], registry);

    const exposition = registry.expose();
    expect(exposition).toContain('# TYPE horizon_listener_ledger_lag gauge');
    expect(exposition).toContain('horizon_listener_ledger_lag 20');
  });

  it('omits the gauge line until the first poll measures it', () => {
    const registry = new MetricsRegistry();
    expect(registry.expose()).not.toContain('ledger_lag');
  });

  it('honours a custom prefix', async () => {
    const registry = new MetricsRegistry({ prefix: 'compliance' });
    await drive([{ events: [makeEvent({ ledger: 100 })], latestLedger: 105 }], registry);
    expect(registry.expose()).toContain('compliance_ledger_lag 5');
  });
});

describe('NoopMetricsRegistry gauge (#812)', () => {
  it('accepts gauge writes and reports nothing', () => {
    const registry = new NoopMetricsRegistry();
    registry.gauge.set('ledger_lag', 42);
    expect(registry.gauge.get('ledger_lag')).toBeUndefined();
    expect(registry.expose()).toBe('');
  });
});
