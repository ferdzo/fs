package cmd

import (
	"context"
	"fs/app"
	"os"
	"os/signal"
	"syscall"
)

func Execute(build BuildInfo) error {
	build = build.normalized()

	if len(os.Args) == 1 {
		return runServerWithSignals()
	}

	root := newRootCommand(build)
	return root.Execute()
}

func runServerWithSignals() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return app.RunServer(ctx)
}
