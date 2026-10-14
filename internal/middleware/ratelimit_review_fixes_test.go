package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"9router/proxy/internal/models"
)

// TestAcquireLeavesQueueOnContextCancel is the regression for the reviewer's
// blocking issue #2: a caller that hangs up must leave the queue, not stay
// parked and then be granted a slot and dispatched upstream for nobody.
func TestAcquireLeavesQueueOnContextCancel(t *testing.T) {
	rl := NewRateLimiter(time.Minute)
	const key = "ctx-cancel"

	// Hold the only slot so the next acquire has to park.
	if _, ok := rl.Acquire(context.Background(), key, 0, 1, 0); !ok {
		t.Fatal("failed to take the only slot")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() {
		_, ok := rl.Acquire(ctx, key, 0, 1, 5*time.Second)
		done <- ok
	}()

	// Let it park, then cancel the caller.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case ok := <-done:
		if ok {
			t.Error("Acquire returned true for a cancelled caller; it should not hold a slot")
		}
	case <-time.After(time.Second):
		t.Fatal("Acquire did not return after the context was cancelled")
	}

	// The waiter must have been discarded: the queue is empty and the slot is
	// still held by the original request only.
	rl.mu.Lock()
	queued := 0
	if q := rl.queues[key]; q != nil {
		queued = len(q.waiters)
	}
	inFlight := int32(0)
	if c := rl.active[key]; c != nil {
		inFlight = atomic.LoadInt32(c)
	}
	rl.mu.Unlock()
	if queued != 0 {
		t.Errorf("queue holds %d waiters after the caller cancelled, want 0", queued)
	}
	if inFlight != 1 {
		t.Errorf("in-flight = %d, want 1 (the cancelled caller leaked a slot)", inFlight)
	}
}

// TestAcquireAlreadyCancelledContextDoesNotPark proves a caller whose context is
// already done never enters the queue at all.
func TestAcquireAlreadyCancelledContextDoesNotPark(t *testing.T) {
	rl := NewRateLimiter(time.Minute)
	const key = "ctx-pre"

	if _, ok := rl.Acquire(context.Background(), key, 0, 1, 0); !ok {
		t.Fatal("failed to take the only slot")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already done

	_, ok := rl.Acquire(ctx, key, 0, 1, 5*time.Second)
	if ok {
		t.Fatal("Acquire admitted a caller whose context was already cancelled")
	}
	rl.mu.Lock()
	queued := 0
	if q := rl.queues[key]; q != nil {
		queued = len(q.waiters)
	}
	rl.mu.Unlock()
	if queued != 0 {
		t.Errorf("queue holds %d waiters, want 0 (a done context must not park)", queued)
	}
}

// TestRefundTPMReturnsQueueRefusalCharge is the regression for the reviewer's
// issue #3: TPM is charged before the queue, so a queue refusal must give the
// charge back. Without it a key under a flood spends its token budget on turns
// that were never served.
func TestRefundTPMReturnsQueueRefusalCharge(t *testing.T) {
	rl := NewRateLimiter(time.Minute)
	const key = "tpm-refund"

	// Charge the bucket directly, as the middleware would before Acquire.
	if !rl.checkTPM(key, 1000, 400, time.Now()) {
		t.Fatal("initial TPM charge refused")
	}
	rl.mu.Lock()
	afterCharge := rl.tokens[key].tokens
	rl.mu.Unlock()
	if afterCharge >= 1000 {
		t.Fatalf("charge did not reduce the bucket: %v", afterCharge)
	}

	rl.RefundTPM(key, 1000, 400)

	rl.mu.Lock()
	afterRefund := rl.tokens[key].tokens
	rl.mu.Unlock()
	if afterRefund <= afterCharge {
		t.Errorf("refund did not return tokens: %v -> %v", afterCharge, afterRefund)
	}
	if afterRefund > 1000 {
		t.Errorf("refund exceeded the bucket limit: %v", afterRefund)
	}
}

// TestRefundTPMIsCappedAtBucketLimit proves a refund can never create more
// headroom than the key was ever given, so a bad refund cannot loosen the limit.
func TestRefundTPMIsCappedAtBucketLimit(t *testing.T) {
	rl := NewRateLimiter(time.Minute)
	const key = "tpm-cap"

	if !rl.checkTPM(key, 100, 10, time.Now()) {
		t.Fatal("initial charge refused")
	}
	// Refund far more than was charged.
	rl.RefundTPM(key, 100, 100000)

	rl.mu.Lock()
	tokens := rl.tokens[key].tokens
	rl.mu.Unlock()
	if tokens > 100 {
		t.Errorf("bucket = %v after an oversized refund, want <= 100", tokens)
	}
}

// TestQueueDepthCapRefuses proves a saturated key's queue is bounded: past the
// cap a request is refused immediately instead of parking another goroutine.
func TestQueueDepthCapRefuses(t *testing.T) {
	rl := NewRateLimiter(time.Minute)
	rl.maxQueueDepth = 3

	const key = "depth"
	if _, ok := rl.Acquire(context.Background(), key, 0, 1, 0); !ok {
		t.Fatal("failed to take the only slot")
	}

	// Fill the queue to the cap; each parks for a long time.
	for i := 0; i < 3; i++ {
		go rl.Acquire(context.Background(), key, 0, 1, 3*time.Second)
	}
	waitForQueueLen(t, rl, key, 3)

	// The next request must be refused immediately, not parked.
	start := time.Now()
	_, ok := rl.Acquire(context.Background(), key, 0, 1, 3*time.Second)
	if ok {
		t.Fatal("Acquire succeeded past the depth cap")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("refusal took %v; a full queue must refuse at once, not park", elapsed)
	}
}

// TestQueueDepthCapThroughMiddleware proves the middleware never parks past the
// cap: the excess request gets a 429 rather than waiting.
func TestQueueDepthCapThroughMiddleware(t *testing.T) {
	rl := NewRateLimiter(time.Minute)
	rl.maxQueueDepth = 1

	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	handler := RequireRateLimit(rl, nil, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-block
		w.WriteHeader(http.StatusOK)
	}))

	mkReq := func() *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		return req.WithContext(withAPIKey(req, &models.APIKey{
			ID:                   "depthmw",
			RateLimitConcurrency: new(1),
			QueueTimeoutMs:       new(5000),
		}))
	}

	// Request 1 holds the slot and blocks in the handler.
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, mkReq())
	}()
	<-entered

	// Request 2 fills the only queue slot (parks).
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, mkReq())
	}()
	waitForQueueLen(t, rl, "depthmw", 1)

	// Request 3 must be refused at once with 429.
	start := time.Now()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, mkReq())
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("third request status = %d, want 429", rec.Code)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("third request took %v; a full queue must refuse fast", elapsed)
	}
	close(block)
}

// TestEvictionSparesAdmittingKey is the regression for the reviewer's issue #6:
// the eviction sweep runs from inside the admit (recordStampLocked) and, at the
// maxKeys boundary, could reclaim the very window the admit is about to stamp —
// the stamp then lands in a fresh window and the key is admitted past its own
// RPM limit. The fix passes the admitting key to the sweep as a skip.
//
// It is asserted on the sweep directly, because that is the unit that changed:
// an end-to-end reproduction depends on which idle keys the sweep happens to
// pick, which is not deterministic. Here the map is exactly at the cap and the
// spare key is named, so the only question is whether the sweep honours it.
func TestEvictionSparesAdmittingKey(t *testing.T) {
	rl := NewRateLimiter(time.Minute)
	rl.maxKeys = 4

	const keep = "admitting-key"
	// Fill the map to the cap, the admitting key among them.
	for _, name := range []string{keep, "idle-1", "idle-2", "idle-3"} {
		rl.mu.Lock()
		rl.windowForLocked(name)
		rl.mu.Unlock()
	}

	rl.mu.Lock()
	before := len(rl.windows)
	rl.evictIfNeededLocked(keep)
	after := len(rl.windows)
	_, kept := rl.windows[keep]
	rl.mu.Unlock()

	if !kept {
		t.Fatal("the admitting key was evicted by its own admit's sweep")
	}
	if after >= before {
		t.Errorf("sweep did not reclaim anything: %d -> %d (test is not exercising eviction)", before, after)
	}
	if after > rl.maxKeys*9/10 {
		t.Errorf("sweep left %d entries, want <= %d", after, rl.maxKeys*9/10)
	}
}
