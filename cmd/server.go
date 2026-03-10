package cmd

import (
	"github.com/spf13/cobra"
)

func newServerCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "server",
		Short: "Run fs object storage server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServerWithSignals()
		},
	}
}
