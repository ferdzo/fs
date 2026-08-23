package auth

import (
	"fs/models"
	"strings"
)

func isAllowed(policy *models.AuthPolicy, target RequestTarget) bool {
	if policy == nil {
		return false
	}

	allowed := false
	for _, stmt := range policy.Statements {
		if !statementMatches(stmt, target) {
			continue
		}
		effect := strings.ToLower(strings.TrimSpace(stmt.Effect))
		if effect == "deny" {
			return false
		}
		if effect == "allow" {
			allowed = true
		}
	}
	return allowed
}

func statementMatches(stmt models.AuthPolicyStatement, target RequestTarget) bool {
	if !actionMatches(stmt.Actions, target.Action) {
		return false
	}
	if !bucketMatches(stmt.Bucket, target.Bucket) {
		return false
	}
	prefix := normalizePrefixPattern(stmt.Prefix)
	if target.Key == "" {
		if target.Action == ActionListBucket {
			return strings.HasPrefix(target.Prefix, prefix)
		}
		return true
	}
	return strings.HasPrefix(target.Key, prefix)
}

// A single trailing "*" in a policy prefix is a wildcard marker; everything
// before it is matched literally. An empty remainder matches any key.
func normalizePrefixPattern(raw string) string {
	return strings.TrimSuffix(strings.TrimSpace(raw), "*")
}

func actionMatches(actions []string, action Action) bool {
	if len(actions) == 0 {
		return false
	}
	for _, current := range actions {
		normalized := strings.TrimSpace(current)
		if normalized == "*" || normalized == "s3:*" || strings.EqualFold(normalized, string(action)) {
			return true
		}
	}
	return false
}

func bucketMatches(pattern, bucket string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || pattern == "*" {
		return true
	}
	return pattern == bucket
}
