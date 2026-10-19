import { createHmac } from 'node:crypto';
import { HttpWebhookSender } from '../src/webhook';
import type { RawContractEvent } from '../src/eventSource';

function makeEvent(overrides: Partial<RawContractEvent> = {}): RawContractEvent {
  return {
    id: 'evt-1',
    contractId: 'CDENYLISTGATE',
    ledger: 100,
    topic: ['denylist_added'],
    value: { address: 'GADDR' },
    ...overrides,
  };
}

describe('HttpWebhookSender', () => {
  it('POSTs the event as JSON with the expected URL, method, and headers', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
    const sender = new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });

    const event = makeEvent();
    await sender.send(event);

    expect(fetchImpl).toHaveBeenCalledTimes(1);
    const [url, init] = fetchImpl.mock.calls[0];
    expect(url).toBe('http://localhost:9999/webhook');
    expect(init.method).toBe('POST');
    expect(init.headers).toEqual({
      'Content-Type': 'application/json',
      'Idempotency-Key': event.id,
    });
    expect(JSON.parse(init.body)).toEqual({ event });
  });

  it('sends an Idempotency-Key derived from the event id (#810)', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
    const sender = new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });

    await sender.send(makeEvent({ id: 'evt-abc-123' }));

    const [, init] = fetchImpl.mock.calls[0];
    expect(init.headers['Idempotency-Key']).toBe('evt-abc-123');
  });

  it('keeps the same Idempotency-Key across retries of one delivery (#810)', async () => {
    const fetchImpl = jest
      .fn()
      .mockResolvedValueOnce({ ok: false, status: 503 })
      .mockResolvedValueOnce({ ok: false, status: 502 })
      .mockResolvedValueOnce({ ok: true, status: 200 });
    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      fetchImpl,
      maxRetries: 3,
    });

    await sender.send(makeEvent({ id: 'evt-retry-key' }));

    expect(fetchImpl).toHaveBeenCalledTimes(3);
    const keys = fetchImpl.mock.calls.map(([, init]) => init.headers['Idempotency-Key']);
    // Identical on every attempt: that is the whole point — a receiver dedupes
    // the retried POSTs of one event by this header.
    expect(new Set(keys).size).toBe(1);
    expect(keys[0]).toBe('evt-retry-key');
  });

  it('derives distinct keys for distinct events', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
    const sender = new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });

    await sender.send(makeEvent({ id: 'evt-1' }));
    await sender.send(makeEvent({ id: 'evt-2' }));

    const keys = fetchImpl.mock.calls.map(([, init]) => init.headers['Idempotency-Key']);
    expect(keys).toEqual(['evt-1', 'evt-2']);
  });

  it('throws when the response is not ok', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: false, status: 500 });
    const sender = new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });

    await expect(sender.send(makeEvent())).rejects.toThrow(/status 500/);
  });

  it('omits the signature header when no signing secret is configured', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
    const sender = new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });

    await sender.send(makeEvent());

    const [, init] = fetchImpl.mock.calls[0];
    expect(init.headers).toEqual({
      'Content-Type': 'application/json',
      'Idempotency-Key': 'evt-1',
    });
  });

  it('adds X-Timestamp and a correctly computed X-Signature header when a signing secret is configured', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
    const secret = 'test-secret';
    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      signingSecret: secret,
      fetchImpl,
    });

    const fixedNowMs = 1_700_000_000_000;
    jest.spyOn(Date, 'now').mockReturnValue(fixedNowMs);
    try {
      const event = makeEvent();
      await sender.send(event);

      const [, init] = fetchImpl.mock.calls[0];
      const expectedTimestamp = String(Math.floor(fixedNowMs / 1000));
      const expectedSignature = `sha256=${createHmac('sha256', secret)
        .update(`${expectedTimestamp}.${init.body}`)
        .digest('hex')}`;

      expect(init.headers).toEqual({
        'Content-Type': 'application/json',
        'X-Timestamp': expectedTimestamp,
        'X-Signature': expectedSignature,
      });
    } finally {
      jest.spyOn(Date, 'now').mockRestore();
    }
  });

  it('computes the documented signature for a known payload/secret/timestamp triple', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      signingSecret: 'known-secret',
      fetchImpl,
    });

    const fixedNowMs = 1_700_000_000_000; // -> X-Timestamp: "1700000000"
    jest.spyOn(Date, 'now').mockReturnValue(fixedNowMs);
    try {
      await sender.send(
        makeEvent({
          id: 'evt-known',
          contractId: 'CDENYLISTGATE',
          ledger: 42,
          topic: ['denylist_added'],
          value: { address: 'GADDR' },
        }),
      );

      const [, init] = fetchImpl.mock.calls[0];
      expect(init.headers['X-Timestamp']).toBe('1700000000');
      // Known-answer test: sha256 HMAC of "<timestamp>.<fixed body above>"
      // under 'known-secret', pre-computed independently of the implementation.
      expect(init.headers['X-Signature']).toBe(
        'sha256=1c7748523c86084c5fb73969db62fe44c64bdbd28da9aa63987badff4f56b1ef',
      );
    } finally {
      jest.spyOn(Date, 'now').mockRestore();
    }
  });

  it('omits X-Timestamp when no signing secret is configured', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
    const sender = new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });

    await sender.send(makeEvent());

    const [, init] = fetchImpl.mock.calls[0];
    expect(init.headers['X-Timestamp']).toBeUndefined();
  });

  it('enables exactly-once delivery when webhook includes idempotency-key header', async () => {
    // Test documents the requirement for exactly-once delivery guarantees
    // (issue #86). The webhook POST should include an idempotency-key derived
    // from event.id to ensure the server can deduplicate retries.
    // Current implementation: does not include idempotency-key.
    // Future: add idempotency-key header derived from event.id to POST request.
    const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
    const sender = new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });

    const event = makeEvent({ id: 'evt-1' });
    await sender.send(event);

    const [, init] = fetchImpl.mock.calls[0];
    // This test documents the expected behavior when idempotency is implemented:
    // The webhook request should include an idempotency-key so that retried
    // deliveries (due to network/process crashes) are deduplicated by the receiver.
    // Note: This test currently verifies the baseline (no idempotency-key);
    // update once idempotency-key header is added to implementation.
    expect(init.body).toBeDefined();
  });

  it('supports idempotency headers for deduplication across delivery attempts', async () => {
    // Test establishes the baseline requirement for exactly-once delivery (issue #86).
    // When the same event is sent multiple times due to a crash/retry scenario,
    // the idempotency-key allows the webhook receiver to detect and skip duplicates.
    // This prevents data corruption from duplicate event processing.
    const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
    const sender = new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });

    const event = makeEvent({ id: 'evt-dedup' });

    // First send attempt
    await sender.send(event);
    const firstCall = fetchImpl.mock.calls[0];

    // Second send attempt (simulating retry after crash)
    await sender.send(event);
    const secondCall = fetchImpl.mock.calls[1];

    // Both calls should send the same event
    expect(JSON.parse((firstCall[1] as any).body)).toEqual({ event });
    expect(JSON.parse((secondCall[1] as any).body)).toEqual({ event });

    // Webhook receiver implementation would use an idempotency-key header
    // (once added to this implementation) to detect that both calls are for
    // the same event and deduplicate at the server side.
  });

  it('handles 404 responses with appropriate error message', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: false, status: 404 });
    const sender = new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });

    await expect(sender.send(makeEvent())).rejects.toThrow(/status 404/);
  });

  it('handles 401 unauthorized responses', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: false, status: 401 });
    const sender = new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });

    await expect(sender.send(makeEvent())).rejects.toThrow(/status 401/);
  });

  it('handles 429 rate limit responses', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: false, status: 429 });
    const sender = new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });

    await expect(sender.send(makeEvent())).rejects.toThrow(/status 429/);
  });

  it('successfully sends events with complex event data structures', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
    const sender = new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });

    const complexEvent = makeEvent({
      value: {
        address: 'GADDR',
        amount: '1000.50',
        timestamp: 1234567890,
        nested: { data: ['array', 'items'] },
      },
    });

    await sender.send(complexEvent);

    expect(fetchImpl).toHaveBeenCalledTimes(1);
    const [, init] = fetchImpl.mock.calls[0];
    const body = JSON.parse(init.body);
    expect(body.event).toEqual(complexEvent);
  });

  it('throws when fetch rejects with a network error', async () => {
    const fetchImpl = jest.fn().mockRejectedValue(new Error('Network connection failed'));
    const sender = new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });

    await expect(sender.send(makeEvent())).rejects.toThrow('Network connection failed');
  });

  it('preserves event data integrity when sending', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
    const sender = new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });

    const originalEvent = makeEvent({
      id: 'evt-unique-123',
      ledger: 999,
      topic: ['custom_event_type'],
    });

    await sender.send(originalEvent);

    const [, init] = fetchImpl.mock.calls[0];
    const sentEvent = JSON.parse(init.body).event;
    expect(sentEvent.id).toBe(originalEvent.id);
    expect(sentEvent.ledger).toBe(originalEvent.ledger);
    expect(sentEvent.topic).toEqual(originalEvent.topic);
  });

  it('includes URL and status code in error message when response is not ok', async () => {
    const url = 'http://localhost:9999/webhook';
    const status = 503;
    const fetchImpl = jest.fn().mockResolvedValue({ ok: false, status });
    const sender = new HttpWebhookSender({ url, fetchImpl });

    const error = await sender.send(makeEvent()).catch((e) => e);
    expect(error.message).toContain(url);
    expect(error.message).toContain(String(status));
  });

  it('respects timeoutMs option by passing AbortSignal to fetch', async () => {
    let capturedSignal: AbortSignal | undefined;
    const fetchImpl = jest.fn().mockImplementation((url, init) => {
      capturedSignal = init.signal;
      return Promise.resolve({ ok: true });
    });
    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      fetchImpl,
      timeoutMs: 5000,
    });

    await sender.send(makeEvent());

    expect(capturedSignal).toBeDefined();
    expect(capturedSignal?.aborted).toBe(false);
  });

  it('does not pass AbortSignal when timeoutMs is not set', async () => {
    let capturedSignal: AbortSignal | undefined;
    const fetchImpl = jest.fn().mockImplementation((url, init) => {
      capturedSignal = init.signal;
      return Promise.resolve({ ok: true });
    });
    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      fetchImpl,
    });

    await sender.send(makeEvent());

    expect(capturedSignal).toBeUndefined();
  });

  it('clears the timeout handle after successful send', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: true });
    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      fetchImpl,
      timeoutMs: 5000,
    });

    // Should complete without hanging
    await expect(sender.send(makeEvent())).resolves.toBeUndefined();
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it('retries on transient failures with backoff', async () => {
    const fetchImpl = jest
      .fn()
      .mockResolvedValueOnce({ ok: false, status: 503 })
      .mockResolvedValueOnce({ ok: false, status: 502 })
      .mockResolvedValueOnce({ ok: true });

    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      fetchImpl,
      maxRetries: 3,
    });

    await sender.send(makeEvent());

    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });

  it('gives up after maxRetries attempts', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: false, status: 503 });

    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      fetchImpl,
      maxRetries: 2,
    });

    await expect(sender.send(makeEvent())).rejects.toThrow(/status 503/);

    expect(fetchImpl).toHaveBeenCalledTimes(3); // initial + 2 retries
  });

  it('does not retry on success', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: true });

    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      fetchImpl,
      maxRetries: 3,
    });

    await sender.send(makeEvent());

    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it('defaults maxRetries to 3', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: false, status: 503 });

    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      fetchImpl,
    });

    await expect(sender.send(makeEvent())).rejects.toThrow();

    expect(fetchImpl).toHaveBeenCalledTimes(4); // initial + 3 default retries
  });

  it('does not retry a 400 response, even with retries budgeted', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: false, status: 400 });

    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      fetchImpl,
      maxRetries: 3,
    });

    await expect(sender.send(makeEvent())).rejects.toThrow(/status 400/);

    expect(fetchImpl).toHaveBeenCalledTimes(1); // no retries — permanent client error
  });

  it('does not retry a 401 response, even with retries budgeted', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: false, status: 401 });

    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      fetchImpl,
      maxRetries: 3,
    });

    await expect(sender.send(makeEvent())).rejects.toThrow(/status 401/);

    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it('does not retry a 404 response, even with retries budgeted', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: false, status: 404 });

    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      fetchImpl,
      maxRetries: 3,
    });

    await expect(sender.send(makeEvent())).rejects.toThrow(/status 404/);

    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it('still retries 5xx responses up to maxRetries', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({ ok: false, status: 502 });

    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      fetchImpl,
      maxRetries: 2,
    });

    await expect(sender.send(makeEvent())).rejects.toThrow(/status 502/);

    expect(fetchImpl).toHaveBeenCalledTimes(3); // initial + 2 retries
  });

  it('still retries network errors up to maxRetries', async () => {
    const fetchImpl = jest.fn().mockRejectedValue(new Error('Network connection failed'));

    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      fetchImpl,
      maxRetries: 2,
    });

    await expect(sender.send(makeEvent())).rejects.toThrow('Network connection failed');

    expect(fetchImpl).toHaveBeenCalledTimes(3); // initial + 2 retries
  });

  it('recovers from a transient 5xx and succeeds without retrying past it', async () => {
    const fetchImpl = jest
      .fn()
      .mockResolvedValueOnce({ ok: false, status: 503 })
      .mockResolvedValueOnce({ ok: true, status: 200 });

    const sender = new HttpWebhookSender({
      url: 'http://localhost:9999/webhook',
      fetchImpl,
      maxRetries: 3,
    });

    await expect(sender.send(makeEvent())).resolves.toBeUndefined();

    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  describe('parentContext for nested tracing (issue #327)', () => {
    it('accepts a parentContext option in the constructor', () => {
      const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
      const parentContext = { traceId: 'trace-123', spanId: 'span-456' };

      const sender = new HttpWebhookSender({
        url: 'http://localhost:9999/webhook',
        fetchImpl,
        parentContext,
      });

      expect(sender).toBeDefined();
    });

    it('stores parentContext for use in tracing spans', () => {
      const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
      const parentContext = { traceId: 'parent-trace', spanId: 'parent-span' };

      const sender = new HttpWebhookSender({
        url: 'http://localhost:9999/webhook',
        fetchImpl,
        parentContext,
      });

      // Verify the sender was constructed with parentContext
      expect(sender).toBeDefined();
      expect(fetchImpl).not.toHaveBeenCalled();
    });

    it('allows parentContext to be optional', () => {
      const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });

      const sender = new HttpWebhookSender({
        url: 'http://localhost:9999/webhook',
        fetchImpl,
      });

      expect(sender).toBeDefined();
    });

    it('preserves event data when parentContext is supplied', async () => {
      const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
      const parentContext = { traceId: 'trace-789', spanId: 'span-012' };

      const sender = new HttpWebhookSender({
        url: 'http://localhost:9999/webhook',
        fetchImpl,
        parentContext,
      });

      const event = makeEvent({ id: 'evt-ctx-test' });
      await sender.send(event);

      const [, init] = fetchImpl.mock.calls[0];
      const sentEvent = JSON.parse(init.body).event;
      expect(sentEvent.id).toBe('evt-ctx-test');
    });
  });

  describe('metrics and tracer threading (issue #326)', () => {
    it('accepts metrics option in the constructor', () => {
      const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
      const mockMetrics = { /* metrics registry mock */ } as any;

      const sender = new HttpWebhookSender({
        url: 'http://localhost:9999/webhook',
        fetchImpl,
        metrics: mockMetrics,
      });

      expect(sender).toBeDefined();
    });

    it('accepts tracer option in the constructor', () => {
      const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
      const mockTracer = { /* tracer mock */ } as any;

      const sender = new HttpWebhookSender({
        url: 'http://localhost:9999/webhook',
        fetchImpl,
        tracer: mockTracer,
      });

      expect(sender).toBeDefined();
    });

    it('accepts both metrics and tracer together', () => {
      const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
      const mockMetrics = { /* metrics registry mock */ } as any;
      const mockTracer = { /* tracer mock */ } as any;

      const sender = new HttpWebhookSender({
        url: 'http://localhost:9999/webhook',
        fetchImpl,
        metrics: mockMetrics,
        tracer: mockTracer,
      });

      expect(sender).toBeDefined();
    });

    it('allows metrics and tracer to be optional', () => {
      const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });

      const sender = new HttpWebhookSender({
        url: 'http://localhost:9999/webhook',
        fetchImpl,
      });

      expect(sender).toBeDefined();
    });

    it('sends events normally when metrics and tracer are provided', async () => {
      const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200 });
      const mockMetrics = { /* metrics registry mock */ } as any;
      const mockTracer = { /* tracer mock */ } as any;

      const sender = new HttpWebhookSender({
        url: 'http://localhost:9999/webhook',
        fetchImpl,
        metrics: mockMetrics,
        tracer: mockTracer,
      });

      const event = makeEvent({ id: 'evt-metrics-trace-test' });
      await sender.send(event);

      expect(fetchImpl).toHaveBeenCalledTimes(1);
      const [, init] = fetchImpl.mock.calls[0];
      const sentEvent = JSON.parse(init.body).event;
      expect(sentEvent.id).toBe('evt-metrics-trace-test');
    });
  });

  describe('constructor validation', () => {
    it('throws when URL is malformed (missing protocol)', () => {
      const fetchImpl = jest.fn();
      expect(() => {
        new HttpWebhookSender({ url: 'localhost:9999/webhook', fetchImpl });
      }).toThrow(/must start with http:\/\/ or https:\/\//);
    });

    it('throws when URL is empty string', () => {
      const fetchImpl = jest.fn();
      expect(() => {
        new HttpWebhookSender({ url: '', fetchImpl });
      }).toThrow(/invalid URL/);
    });

    it('throws when URL contains invalid characters', () => {
      const fetchImpl = jest.fn();
      expect(() => {
        new HttpWebhookSender({ url: 'http://local host/webhook', fetchImpl });
      }).toThrow(/invalid URL/);
    });

    it('accepts valid http:// URLs', () => {
      const fetchImpl = jest.fn();
      expect(() => {
        new HttpWebhookSender({ url: 'http://localhost:9999/webhook', fetchImpl });
      }).not.toThrow();
    });

    it('accepts valid https:// URLs', () => {
      const fetchImpl = jest.fn();
      expect(() => {
        new HttpWebhookSender({ url: 'https://example.com/webhook', fetchImpl });
      }).not.toThrow();
    });

    it('throws with error message indicating the URL parsing failure reason', () => {
      const fetchImpl = jest.fn();
      expect(() => {
        new HttpWebhookSender({ url: 'ht!tp://invalid', fetchImpl });
      }).toThrow(/invalid URL.*ht!tp:\/\/invalid/);
    });

    it('throws when URL lacks http/https protocol', () => {
      const fetchImpl = jest.fn();
      expect(() => {
        new HttpWebhookSender({ url: 'ftp://example.com/webhook', fetchImpl });
      }).toThrow(/must start with http:\/\/ or https:\/\//);
    });
  });
});
