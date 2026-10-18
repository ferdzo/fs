package api

import (
	"io"
	"net/http"
	"time"
)

// bodyReadTimeoutMiddleware arms an idle deadline on request-body reads.
//
// serverReadTimeout is intentionally zero so multi-gigabyte uploads are never
// truncated mid-flight, but that allows a signed request declaring a large
// Content-Length with no body to hold one of the finite listener slots
// forever. This wrapper arms a read deadline as soon as headers complete and
// refreshes it on every successful body read: active uploads keep streaming,
// stalled ones are reaped and their slot returns to the pool.
func bodyReadTimeoutMiddleware(d time.Duration) func(http.Handler) http.Handler {
	if d <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rc := http.NewResponseController(w)
			_ = rc.SetReadDeadline(time.Now().Add(d))

			if r.Body != nil && r.Body != http.NoBody {
				r.Body = &deadlineRefreshReader{inner: r.Body, rc: rc, d: d}
			}

			defer func() {
				// Do not clear the deadline here. net/http drains any request
				// body the handler left unread *after* the handler returns
				// (finishRequest), and clearing it re-opens the stall this
				// middleware exists to bound. Re-arming keeps that drain
				// bounded; net/http installs the idle/read-header deadline
				// before reading the next keepalive request, so the deadline
				// does not leak across requests.
				_ = rc.SetReadDeadline(time.Now().Add(d))
			}()

			next.ServeHTTP(w, r)
		})
	}
}

// deadlineRefreshReader pushes the connection read deadline forward on every
// successful read. It clears the deadline on a terminal read so the rest of the
// handler is not constrained; the middleware re-arms a bounded deadline when
// the handler returns so net/http's post-handler drain cannot stall.
type deadlineRefreshReader struct {
	inner io.ReadCloser
	rc    *http.ResponseController
	d     time.Duration
}

func (r *deadlineRefreshReader) Read(p []byte) (int, error) {
	n, err := r.inner.Read(p)
	if n > 0 {
		_ = r.rc.SetReadDeadline(time.Now().Add(r.d))
	}
	if err != nil {
		_ = r.rc.SetReadDeadline(time.Time{})
	}
	return n, err
}

func (r *deadlineRefreshReader) Close() error { return r.inner.Close() }
