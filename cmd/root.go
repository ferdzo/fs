package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

func newRootCommand(build BuildInfo) *cobra.Command {
	root := &cobra.Command{
		Use:           "fs",
		Short:         "fs object storage server and admin CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       build.Version,
	}

	root.SetVersionTemplate(versionTemplate(build))
	root.AddCommand(newServerCommand())
	root.AddCommand(newAdminCommand(build))
	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print build and runtime version information",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "version=%s commit=%s date=%s go=%s\n", build.Version, build.Commit, build.Date, runtime.Version())
			return err
		},
	})

	return root
}

func versionTemplate(build BuildInfo) string {
	return fmt.Sprintf("version=%s commit=%s date=%s\n", build.Version, build.Commit, build.Date)
}
