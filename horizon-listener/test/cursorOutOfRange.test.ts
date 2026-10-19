import { HorizonListener } from '../src/listener';
import type { EventSource, RawContractEvent } from '../src/eventSource';
import {
  CursorOutOfRangeError,
  extractOldestAvailableLedger,
  isCursorOutOfRangeError,
} from '../src/errors';

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

/** Minimal logger that records what the listener reported. */
function makeLogger() {
  return { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() };
}

describe('CursorOutOfRangeError (#813)', () => {
  it('carries what was requested and what the node still has', () => {
    const error = new CursorOutOfRangeError('cursor-abc', 1234);
    expect(error.name).toBe('CursorOutOfRangeError');
    expect(error.requested).toBe('cursor-abc');
    expect(error.oldestAvailable).toBe(1234);
    expect(error.message).toContain('1234');
    expect(error.message).toContain('retention window');
  });

  it('says so when the node did not report its oldest ledger', () => {
    const error = new CursorOutOfRangeError(500);
    expect(error.oldestAvailable).toBeUndefined();
    expect(error.message).toContain('did not report');
  });

  it.each([
    'cursor is out of range',
    'startLedger 100 is outside the ledger range',
    'requested ledger is outside of the ledger range of the node',
    'the ledger is pruned from this node',
    'position outside the retention window',
  ])('recognises the node message: %s', (message) => {
    expect(isCursorOutOfRangeError(new Error(message))).toBe(true);
  });

  it.each([
    'ECONNREFUSED',
    'timeout after 30000ms',
    'Internal error',
  ])('does not treat unrelated failures as gaps: %s', (message) => {
    expect(isCursorOutOfRangeError(new Error(message))).toBe(false);
  });

  it('extracts the oldest available ledger when the message includes it', () => {
    expect(extractOldestAvailableLedger(new Error('ledger range: oldest available ledger is 987'))).toBe(987);
    expect(extractOldestAvailableLedger(new Error('outside the ledger range 640 to 700'))).toBe(640);
    expect(extractOldestAvailableLedger(new Error('cursor out of range'))).toBeUndefined();
  });
});

describe('listener gap handling (#813)', () => {
  it('stops and throws instead of retrying pruned history in fail mode', async () => {
    const getEvents = jest.fn().mockRejectedValue(new CursorOutOfRangeError('cursor-old', 900));
    const listener: HorizonListener = new HorizonListener({
      eventSource: { getEvents } as EventSource,
      onEvent: () => {},
      sleep: async () => {},
      pollIntervalMs: 1000,
      maxRetries: 5,
    });

    await expect(listener.start()).rejects.toBeInstanceOf(CursorOutOfRangeError);
    // The whole point: one attempt, not five retries of an impossible position.
    expect(getEvents).toHaveBeenCalledTimes(1);
  });

  it('surfaces a plain out-of-range error as the typed one', async () => {
    const getEvents = jest.fn().mockRejectedValue(new Error('cursor is out of range'));
    const listener: HorizonListener = new HorizonListener({
      eventSource: { getEvents } as EventSource,
      onEvent: () => {},
      sleep: async () => {},
    });

    await expect(listener.start()).rejects.toBeInstanceOf(CursorOutOfRangeError);
  });

  it('resumes from the oldest available ledger in skipToOldest mode', async () => {
    const restart = jest.fn();
    const getEvents = jest
      .fn()
      .mockRejectedValueOnce(new CursorOutOfRangeError('cursor-old', 900))
      .mockResolvedValueOnce({ events: [makeEvent({ ledger: 900 })], nextCursor: 'cursor-new' });

    const listener: HorizonListener = new HorizonListener({
      eventSource: { getEvents, restart } as EventSource,
      onEvent: () => listener.stop(),
      onGap: 'skipToOldest',
      sleep: async () => {},
      logger: makeLogger(),
    });

    await listener.start();

    expect(restart).toHaveBeenCalledWith(900);
    expect(getEvents).toHaveBeenCalledTimes(2);
    // The retry after a gap restart is cursor-less: the source is re-pointed.
    expect(getEvents.mock.calls[1][0]).toBeUndefined();
  });

  it('fails loudly when skipToOldest is asked for but the source cannot restart', async () => {
    const getEvents = jest.fn().mockRejectedValue(new CursorOutOfRangeError('cursor-old', 900));
    const listener: HorizonListener = new HorizonListener({
      eventSource: { getEvents } as EventSource,
      onEvent: () => {},
      onGap: 'skipToOldest',
      sleep: async () => {},
      logger: makeLogger(),
    });

    await expect(listener.start()).rejects.toBeInstanceOf(CursorOutOfRangeError);
  });

  it('fails when the node did not report the oldest ledger it still has', async () => {
    const restart = jest.fn();
    const getEvents = jest.fn().mockRejectedValue(new CursorOutOfRangeError('cursor-old'));
    const logger = makeLogger();
    const listener: HorizonListener = new HorizonListener({
      eventSource: { getEvents, restart } as EventSource,
      onEvent: () => {},
      onGap: 'skipToOldest',
      sleep: async () => {},
      logger,
    });

    await expect(listener.start()).rejects.toBeInstanceOf(CursorOutOfRangeError);
    expect(restart).not.toHaveBeenCalled();
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('oldest available ledger'));
  });

  it('still retries ordinary transient failures in fail mode', async () => {
    const getEvents = jest
      .fn()
      .mockRejectedValueOnce(new Error('ECONNRESET'))
      .mockResolvedValueOnce({ events: [makeEvent()], nextCursor: 'c' });
    const listener: HorizonListener = new HorizonListener({
      eventSource: { getEvents } as EventSource,
      onEvent: () => listener.stop(),
      sleep: async () => {},
      maxRetries: 3,
    });

    await listener.start();
    expect(getEvents).toHaveBeenCalledTimes(2);
  });
});
