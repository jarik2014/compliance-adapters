import { HorizonListener } from '../src/listener';
import type { EventSource, RawContractEvent } from '../src/eventSource';
import { MetricsRegistry } from '../src/metrics';

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

function makeListener(eventSource: EventSource, extra: Record<string, unknown> = {}) {
  return new HorizonListener({
    eventSource,
    onEvent: () => {},
    pollIntervalMs: 1000,
    sleep: async () => {},
    ...extra,
  });
}

describe('listener.events() async iterator (#815)', () => {
  it('yields events in order across pages', async () => {
    const getEvents = jest
      .fn()
      .mockResolvedValueOnce({ events: [makeEvent({ id: 'a' }), makeEvent({ id: 'b' })], nextCursor: 'c1' })
      .mockResolvedValueOnce({ events: [makeEvent({ id: 'c' })], nextCursor: 'c2' });

    const listener = makeListener({ getEvents } as EventSource);
    const seen: string[] = [];

    for await (const event of listener.events()) {
      seen.push(event.id);
      if (seen.length === 3) break;
    }

    expect(seen).toEqual(['a', 'b', 'c']);
  });

  it('advances the cursor only after a page is fully consumed', async () => {
    const getEvents = jest
      .fn()
      .mockResolvedValueOnce({ events: [makeEvent({ id: 'a' }), makeEvent({ id: 'b' })], nextCursor: 'c1' })
      .mockResolvedValueOnce({ events: [makeEvent({ id: 'c' })], nextCursor: 'c2' });

    const listener = makeListener({ getEvents } as EventSource);
    const iterator = listener.events();

    await iterator.next(); // 'a'
    expect(getEvents).toHaveBeenCalledTimes(1);
    expect(getEvents).toHaveBeenLastCalledWith(undefined);

    await iterator.next(); // 'b'
    const third = await iterator.next(); // page exhausted → fetch with c1
    expect(third.value?.id).toBe('c');
    expect(getEvents).toHaveBeenCalledTimes(2);
    expect(getEvents).toHaveBeenLastCalledWith('c1');

    await iterator.return?.();
  });

  it('stops polling when the consumer breaks out of the loop', async () => {
    const getEvents = jest.fn().mockResolvedValue({
      events: [makeEvent({ id: 'a' }), makeEvent({ id: 'b' })],
      nextCursor: 'c1',
    });

    const listener = makeListener({ getEvents } as EventSource);
    for await (const _event of listener.events()) {
      break;
    }

    // One page was enough: breaking out must not trigger another poll.
    expect(getEvents).toHaveBeenCalledTimes(1);
  });

  it('stops when the AbortSignal fires, including during a quiet period', async () => {
    const controller = new AbortController();
    const getEvents = jest.fn().mockResolvedValue({ events: [], nextCursor: 'c1' });

    const listener = new HorizonListener({
      eventSource: { getEvents } as EventSource,
      onEvent: () => {},
      pollIntervalMs: 1000,
      // Abort while the iterator is waiting between polls.
      sleep: async () => {
        controller.abort();
      },
    });

    const seen: RawContractEvent[] = [];
    for await (const event of listener.events({ signal: controller.signal })) {
      seen.push(event);
    }

    expect(seen).toEqual([]);
    expect(controller.signal.aborted).toBe(true);
  });

  it('does not poll ahead of a slow consumer (backpressure)', async () => {
    const getEvents = jest
      .fn()
      .mockResolvedValueOnce({
        events: [makeEvent({ id: 'a' }), makeEvent({ id: 'b' })],
        nextCursor: 'c1',
      })
      .mockResolvedValueOnce({ events: [makeEvent({ id: 'c' })], nextCursor: 'c2' });

    const listener = makeListener({ getEvents } as EventSource);
    const iterator = listener.events();

    await iterator.next(); // a — page 1 fetched
    // The consumer is slow here; the listener must not fetch page 2 yet.
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(getEvents).toHaveBeenCalledTimes(1);

    await iterator.next(); // b — page 1 exhausted
    await iterator.next(); // c — only now page 2 is fetched
    expect(getEvents).toHaveBeenCalledTimes(2);

    await iterator.return?.();
  });

  it('updates the ledger-lag gauge from iterator polls', async () => {
    const registry = new MetricsRegistry();
    const getEvents = jest
      .fn()
      .mockResolvedValueOnce({ events: [makeEvent({ id: 'a', ledger: 100 })], nextCursor: 'c1', latestLedger: 130 })
      .mockResolvedValueOnce({ events: [], nextCursor: 'c1' });

    const listener = makeListener({ getEvents } as EventSource, { metrics: registry });
    const iterator = listener.events();

    await iterator.next();
    expect(registry.gauge.get('ledger_lag')).toBe(30);

    await iterator.return?.();
  });
});
