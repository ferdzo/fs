package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func newAdminUserCommand(opts *adminOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "user",
		Short: "Manage auth users",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newAdminUserCreateCommand(opts))
	cmd.AddCommand(newAdminUserListCommand(opts))
	cmd.AddCommand(newAdminUserGetCommand(opts))
	cmd.AddCommand(newAdminUserDeleteCommand(opts))
	cmd.AddCommand(newAdminUserSetStatusCommand(opts))
	cmd.AddCommand(newAdminUserSetRoleCommand(opts))
	return cmd
}

func newAdminUserCreateCommand(opts *adminOptions) *cobra.Command {
	var (
		accessKey string
		secretKey string
		status    string
		role      string
		bucket    string
		prefix    string
	)

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a user",
		RunE: func(cmd *cobra.Command, args []string) error {
			accessKey = strings.TrimSpace(accessKey)
			if accessKey == "" {
				return usageError("fs admin user create --access-key <id> --role admin|readwrite|readonly [--status active|disabled] [--bucket <name>] [--prefix <path>]", "--access-key is required")
			}
			policy, err := buildPolicyFromRole(rolePolicyOptions{
				Role:   role,
				Bucket: bucket,
				Prefix: prefix,
			})
			if err != nil {
				return usageError("fs admin user create --access-key <id> --role admin|readwrite|readonly [--status active|disabled] [--bucket <name>] [--prefix <path>]", err.Error())
			}

			client, err := newAdminAPIClient(opts, true)
			if err != nil {
				return err
			}

			out, err := client.CreateUser(context.Background(), createUserRequest{
				AccessKeyID: accessKey,
				SecretKey:   strings.TrimSpace(secretKey),
				Status:      strings.TrimSpace(status),
				Policy:      policy,
			})
			if err != nil {
				return err
			}
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), out)
			}
			if err := writeUserTable(cmd.OutOrStdout(), out, true); err != nil {
				return err
			}
			if strings.TrimSpace(out.SecretKey) != "" {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "\nsecretKey is only returned once during create; store it securely.")
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&accessKey, "access-key", "", "User access key ID")
	cmd.Flags().StringVar(&secretKey, "secret-key", "", "User secret key (optional; auto-generated when omitted)")
	cmd.Flags().StringVar(&status, "status", "active", "User status: active|disabled")
	cmd.Flags().StringVar(&role, "role", "readwrite", "Role: admin|readwrite|readonly")
	cmd.Flags().StringVar(&bucket, "bucket", "*", "Bucket scope, defaults to *")
	cmd.Flags().StringVar(&prefix, "prefix", "*", "Prefix scope, defaults to *")
	return cmd
}

func newAdminUserListCommand(opts *adminOptions) *cobra.Command {
	var (
		limit  int
		cursor string
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List users",
		RunE: func(cmd *cobra.Command, args []string) error {
			if limit < 1 || limit > 1000 {
				return usageError("fs admin user list [--limit 1-1000] [--cursor <token>]", "--limit must be between 1 and 1000")
			}
			client, err := newAdminAPIClient(opts, true)
			if err != nil {
				return err
			}
			out, err := client.ListUsers(context.Background(), limit, cursor)
			if err != nil {
				return err
			}
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), out)
			}
			return writeUserListTable(cmd.OutOrStdout(), out)
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 100, "List page size (1-1000)")
	cmd.Flags().StringVar(&cursor, "cursor", "", "Pagination cursor from previous list call")
	return cmd
}

func newAdminUserGetCommand(opts *adminOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <access-key-id>",
		Short: "Get one user",
		Args:  requireAccessKeyArg("fs admin user get <access-key-id>"),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newAdminAPIClient(opts, true)
			if err != nil {
				return err
			}
			out, err := client.GetUser(context.Background(), args[0])
			if err != nil {
				return err
			}
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), out)
			}
			return writeUserTable(cmd.OutOrStdout(), out, false)
		},
	}
	return cmd
}

func newAdminUserDeleteCommand(opts *adminOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <access-key-id>",
		Short: "Delete one user",
		Args:  requireAccessKeyArg("fs admin user delete <access-key-id>"),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newAdminAPIClient(opts, true)
			if err != nil {
				return err
			}
			if err := client.DeleteUser(context.Background(), args[0]); err != nil {
				return err
			}
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), map[string]string{
					"status":      "deleted",
					"accessKeyId": args[0],
				})
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "deleted user %s\n", args[0])
			return err
		},
	}
	return cmd
}

func newAdminUserSetStatusCommand(opts *adminOptions) *cobra.Command {
	var status string
	cmd := &cobra.Command{
		Use:   "set-status <access-key-id>",
		Short: "Set user status",
		Args:  requireAccessKeyArg("fs admin user set-status <access-key-id> --status active|disabled"),
		RunE: func(cmd *cobra.Command, args []string) error {
			status = strings.TrimSpace(status)
			if status == "" {
				return usageError("fs admin user set-status <access-key-id> --status active|disabled", "--status is required")
			}
			normalized := strings.ToLower(status)
			if normalized != "active" && normalized != "disabled" {
				return usageError("fs admin user set-status <access-key-id> --status active|disabled", "--status must be active or disabled")
			}
			client, err := newAdminAPIClient(opts, true)
			if err != nil {
				return err
			}
			out, err := client.SetUserStatus(context.Background(), args[0], normalized)
			if err != nil {
				return err
			}
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), out)
			}
			return writeUserTable(cmd.OutOrStdout(), out, false)
		},
	}
	cmd.Flags().StringVar(&status, "status", "", "User status: active|disabled")
	return cmd
}

func newAdminUserSetRoleCommand(opts *adminOptions) *cobra.Command {
	var (
		role    string
		bucket  string
		prefix  string
		replace bool
	)
	cmd := &cobra.Command{
		Use:   "set-role <access-key-id>",
		Short: "Add or replace user role policy statement",
		Args:  requireAccessKeyArg("fs admin user set-role <access-key-id> --role admin|readwrite|readonly [--bucket <name>] [--prefix <path>] [--replace]"),
		RunE: func(cmd *cobra.Command, args []string) error {
			policy, err := buildPolicyFromRole(rolePolicyOptions{
				Role:   role,
				Bucket: bucket,
				Prefix: prefix,
			})
			if err != nil {
				return usageError("fs admin user set-role <access-key-id> --role admin|readwrite|readonly [--bucket <name>] [--prefix <path>] [--replace]", err.Error())
			}

			client, err := newAdminAPIClient(opts, true)
			if err != nil {
				return err
			}

			finalPolicy := policy
			if !replace {
				existing, err := client.GetUser(context.Background(), args[0])
				if err != nil {
					return err
				}
				finalPolicy = mergePolicyStatements(existing.Policy, policy)
			}

			out, err := client.SetUserPolicy(context.Background(), args[0], finalPolicy)
			if err != nil {
				return err
			}
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), out)
			}
			return writeUserTable(cmd.OutOrStdout(), out, false)
		},
	}
	cmd.Flags().StringVar(&role, "role", "readwrite", "Role: admin|readwrite|readonly")
	cmd.Flags().StringVar(&bucket, "bucket", "*", "Bucket scope, defaults to *")
	cmd.Flags().StringVar(&prefix, "prefix", "*", "Prefix scope, defaults to *")
	cmd.Flags().BoolVar(&replace, "replace", false, "Replace all existing policy statements instead of appending")
	return cmd
}

func mergePolicyStatements(existing *adminPolicy, addition adminPolicy) adminPolicy {
	merged := adminPolicy{}
	if existing != nil {
		merged.Principal = existing.Principal
		merged.Statements = append(merged.Statements, existing.Statements...)
	}

	if len(addition.Statements) == 0 {
		return merged
	}

	stmt := addition.Statements[0]
	for _, current := range merged.Statements {
		if policyStatementsEqual(current, stmt) {
			return merged
		}
	}
	merged.Statements = append(merged.Statements, stmt)
	return merged
}

func policyStatementsEqual(a, b adminPolicyStatement) bool {
	if a.Effect != b.Effect || a.Bucket != b.Bucket || a.Prefix != b.Prefix {
		return false
	}
	if len(a.Actions) != len(b.Actions) {
		return false
	}
	for i := range a.Actions {
		if a.Actions[i] != b.Actions[i] {
			return false
		}
	}
	return true
}

func requireAccessKeyArg(usage string) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return usageError(usage, "missing or invalid <access-key-id> argument")
		}
		if strings.TrimSpace(args[0]) == "" {
			return usageError(usage, "access key id cannot be empty")
		}
		return nil
	}
}

func usageError(usage, message string) error {
	return fmt.Errorf("%s\nusage: %s", message, usage)
}
