package utils

import (
	"log/slog"
	"runtime/debug"
)

// Go launches fn in a new goroutine with panic recovery. A panic in a
// background pump (chunked decoding, object streaming) must not crash the
// whole server, so it is logged instead.
func Go(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Default().Error("goroutine_panic_recovered",
					"goroutine", name,
					"panic", r,
					"stack", string(debug.Stack()),
				)
			}
		}()
		fn()
	}()
}
