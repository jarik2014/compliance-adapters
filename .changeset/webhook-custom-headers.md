---
'horizon-listener': minor
---

`HttpWebhookSender` accepts a `headers` option: static headers sent with every
delivery, for receivers behind gateways that require an API key or tenant id.
The map is merged under the reserved headers — `Content-Type`,
`Idempotency-Key`, `X-Timestamp` and `X-Signature` always win — so a custom
entry can never break signing or idempotency.
