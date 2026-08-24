package auth

import (
	"sync"
	"time"
)

// failureLimiter is a per-key sliding-window counter used to throttle
// authentication failures so online guessing cannot run at wire speed.
// A nil *failureLimiter allows everything.
type failureLimiter struct {
	mu     sync.Mutex
	window time.Duration
	limit  int
	hits   map[string][]time.Time
}

func newFailureLimiter(limit int, window time.Duration) *failureLimiter {
	if limit <= 0 || window <= 0 {
		return nil
	}
	return &failureLimiter{
		window: window,
		limit:  limit,
		hits:   make(map[string][]time.Time),
	}
}

// allow records a failure for key and reports whether the caller is still
// under the limit. Entries older than the window are pruned on access.
func (f *failureLimiter) allow(key string) bool {
	if f == nil {
		return true
	}
	now := time.Now()
	cutoff := now.Add(-f.window)

	f.mu.Lock()
	defer f.mu.Unlock()
	arr := f.hits[key]
	i := 0
	for ; i < len(arr) && arr[i].Before(cutoff); i++ {
	}
	arr = arr[i:]
	allowed := len(arr) < f.limit
	if allowed {
		arr = append(arr, now)
	}
	// Cap growth: once the limit is reached the oldest entries age out of the
	// window naturally, so storing more than limit+1 is never useful.
	if len(arr) > f.limit+1 {
		arr = arr[len(arr)-f.limit-1:]
	}
	f.hits[key] = arr

	// Opportunistic janitor: drop keys whose windows fully expired.
	if len(f.hits) > 10_000 {
		for k, v := range f.hits {
			if len(v) == 0 || v[len(v)-1].Before(cutoff) {
				delete(f.hits, k)
			}
		}
	}
	return allowed
}
