package main

import (
	"fs/cmd"
	"os"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	build := cmd.BuildInfo{
		Version: version,
		Commit:  commit,
		Date:    date,
	}
	if err := cmd.Execute(build); err != nil {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
}
