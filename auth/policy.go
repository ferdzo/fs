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
	if target.Key == "" {
		return true
	}

	prefix := strings.TrimSpace(stmt.Prefix)
	if prefix == "" || prefix == "*" {
		return true
	}
	return strings.HasPrefix(target.Key, prefix)
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
