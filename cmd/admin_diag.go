package cmd

import (
	"context"
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

func newAdminDiagCommand(opts *adminOptions, build BuildInfo) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diag",
		Short: "Diagnostics and connectivity checks",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newAdminDiagHealthCommand(opts))
	cmd.AddCommand(newAdminDiagVersionCommand(build, opts))
	return cmd
}

func newAdminDiagHealthCommand(opts *adminOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Check server health endpoint",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newAdminAPIClient(opts, false)
			if err != nil {
				return err
			}
			status, err := client.Health(context.Background())
			if err != nil {
				return err
			}
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), map[string]string{
					"status": status,
				})
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "health: %s\n", status)
			return err
		},
	}
	return cmd
}

func newAdminDiagVersionCommand(build BuildInfo, opts *adminOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print CLI version metadata",
		RunE: func(cmd *cobra.Command, args []string) error {
			out := map[string]string{
				"version": build.Version,
				"commit":  build.Commit,
				"date":    build.Date,
				"go":      runtime.Version(),
			}
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), out)
			}
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "version=%s commit=%s date=%s go=%s\n", out["version"], out["commit"], out["date"], out["go"])
			return err
		},
	}
	return cmd
}
