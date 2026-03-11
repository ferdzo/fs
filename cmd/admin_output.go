package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"
)

func writeJSON(out io.Writer, value any) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func formatUnix(ts int64) string {
	if ts <= 0 {
		return "-"
	}
	return time.Unix(ts, 0).UTC().Format(time.RFC3339)
}

func writeUserListTable(out io.Writer, value *adminUserListResponse) error {
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "ACCESS_KEY_ID\tSTATUS\tCREATED_AT\tUPDATED_AT"); err != nil {
		return err
	}
	for _, item := range value.Items {
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", item.AccessKeyID, item.Status, formatUnix(item.CreatedAt), formatUnix(item.UpdatedAt)); err != nil {
			return err
		}
	}
	if strings.TrimSpace(value.NextCursor) != "" {
		if _, err := fmt.Fprintf(w, "\nNEXT_CURSOR\t%s\t\t\n", value.NextCursor); err != nil {
			return err
		}
	}
	return w.Flush()
}

func writeUserTable(out io.Writer, value *adminUserResponse, includeSecret bool) error {
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintf(w, "accessKeyId\t%s\n", value.AccessKeyID); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "status\t%s\n", value.Status); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "createdAt\t%s\n", formatUnix(value.CreatedAt)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "updatedAt\t%s\n", formatUnix(value.UpdatedAt)); err != nil {
		return err
	}
	if includeSecret && strings.TrimSpace(value.SecretKey) != "" {
		if _, err := fmt.Fprintf(w, "secretKey\t%s\n", value.SecretKey); err != nil {
			return err
		}
	}
	if value.Policy != nil {
		for i, stmt := range value.Policy.Statements {
			idx := i + 1
			if _, err := fmt.Fprintf(w, "policy[%d].effect\t%s\n", idx, stmt.Effect); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "policy[%d].actions\t%s\n", idx, strings.Join(stmt.Actions, ",")); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "policy[%d].bucket\t%s\n", idx, stmt.Bucket); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "policy[%d].prefix\t%s\n", idx, stmt.Prefix); err != nil {
				return err
			}
		}
	}
	return w.Flush()
}
