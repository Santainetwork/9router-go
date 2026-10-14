### ✨ feat(ratelimit): per-key lifetime token quota and FIFO request queue

- **Lifetime token quota**: new `tokenQuota` column per API key. It caps the
  cumulative prompt+completion tokens a key may spend over its whole life;
  once reached, requests are refused with `403` and `X-Token-Quota-Limit` /
  `X-Token-Quota-Used` headers. Usage is summed from `usageHistory` under both
  the masked key this gateway writes and the raw key the upstream dashboard
  wrote. A failed usage read fails **open**, so a storage blip cannot lock a
  key out of a service it has always been able to call.
- **Request queue**: new `queueTimeoutMs` column. A request that exceeds the
  key's RPM or concurrency limit now waits in a **FIFO queue** (head-first, no
  overtaking) for the RPM window to roll or a concurrent slot to free, up to
  the timeout, before returning `429`. Defaults to 60s when a concurrency cap
  is set; a key with no queueable limit keeps the previous immediate-refusal
  behaviour. TPM is deliberately **not** queued — a token bucket refills
  continuously and would live-lock a waiter.
- **Atomic admission**: a request refused on concurrency does not consume an
  RPM stamp, so concurrency pressure cannot silently drain the RPM budget.
- **Observability**: new `router_rate_limit_queue_seconds` histogram records
  how long requests wait for a slot.
- **Dashboard / API / backup**: `tokenQuota` and `queueTimeoutMs` are exposed
  on `GET /api/keys`, accepted on `PUT /api/keys/{id}` (non-negative; omitted
  leaves the stored value unchanged), shown in the key policy dialog with a
  quota badge, and carried through database backup/restore so a restore cannot
  silently lift a throttle. Schema columns are additive; existing installs
  upgrade in place, and `usageHistory` gains an `apiKey` index so the quota
  sum is not a full table scan on every quota-bearing request.
- **No behaviour change for an existing install.** A key with no
  `queueTimeoutMs` of its own only queues when it also has its own concurrency
  cap (the upstream 60s default). A concurrency cap that arrives purely from
  the operator's global default keeps the pre-change immediate refusal, so a
  global setting cannot silently introduce queueing on every key.
- Verified: `go build ./...`, `go vet ./internal/...`, and `go test -count=1
  ./internal/...` all pass; new queue/quota/schema/backup/full-stack tests pass
  under `-race`.
