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
				// The deadline must not leak into the next keepalive request.
				_ = rc.SetReadDeadline(time.Time{})
			}()

			next.ServeHTTP(w, r)
		})
	}
}

// deadlineRefreshReader pushes the connection read deadline forward on every
// successful read and clears it on terminal states so response writes and
// connection teardown are unconstrained.
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
