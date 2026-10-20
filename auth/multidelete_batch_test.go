package auth

import (
	"encoding/base64"
	"errors"
	"fmt"
	"fs/metadata"
	"fs/models"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// countingAuthStore wraps a Store and counts policy lookups so tests can prove
// that batch authorization performs one read regardless of key count.
type countingAuthStore struct {
	Store
	policyReads atomic.Int64
}

func (s *countingAuthStore) GetAuthPolicy(accessKeyID string) (*models.AuthPolicy, error) {
	s.policyReads.Add(1)
	return s.Store.GetAuthPolicy(accessKeyID)
}

func (s *countingAuthStore) resetPolicyReads() {
	s.policyReads.Store(0)
}

func (s *countingAuthStore) policyReadCount() int64 {
	return s.policyReads.Load()
}

func newBatchAuthService(tb testing.TB, enabled bool) (*Service, *countingAuthStore) {
	tb.Helper()

	md, err := metadata.NewMetadataHandler(filepath.Join(tb.TempDir(), "metadata.db"))
	if err != nil {
		tb.Fatalf("new metadata handler: %v", err)
	}
	tb.Cleanup(func() {
		_ = md.Close()
	})

	store := &countingAuthStore{Store: md}
	masterKey := base64.StdEncoding.EncodeToString(make([]byte, 32))
	svc, err := NewService(ConfigFromValues(
		enabled,
		"us-east-1",
		0,
		0,
		masterKey,
		"",
		"",
		"",
		0,
	), store)
	if err != nil {
		tb.Fatalf("new auth service: %v", err)
	}
	return svc, store
}

func createBatchPolicyUser(tb testing.TB, svc *Service, statements []models.AuthPolicyStatement) {
	tb.Helper()

	_, err := svc.CreateUser(CreateUserInput{
		AccessKeyID: "batch-user",
		SecretKey:   "batch-secret-1",
		Policy: models.AuthPolicy{
			Statements: statements,
		},
	})
	if err != nil {
		tb.Fatalf("create batch user: %v", err)
	}
}

func TestAuthorizeBatchReadsPolicyOnceForManyKeys(t *testing.T) {
	svc, store := newBatchAuthService(t, true)
	createBatchPolicyUser(t, svc, []models.AuthPolicyStatement{
		{Effect: "allow", Actions: []string{"s3:DeleteObject"}, Bucket: "test-bucket", Prefix: "allowed/"},
	})

	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("allowed/file-%03d.txt", i)
	}

	errs := svc.AuthorizeBatch("batch-user", ActionDeleteObject, "test-bucket", keys)
	if len(errs) != len(keys) {
		t.Fatalf("AuthorizeBatch returned %d results, want %d", len(errs), len(keys))
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("key %q denied: %v", keys[i], err)
		}
	}
	if got := store.policyReadCount(); got != 1 {
		t.Fatalf("policy reads = %d, want 1 for %d keys", got, len(keys))
	}
}

func TestAuthorizeBatchPreservesPerKeySemantics(t *testing.T) {
	svc, store := newBatchAuthService(t, true)
	createBatchPolicyUser(t, svc, []models.AuthPolicyStatement{
		{Effect: "allow", Actions: []string{"s3:DeleteObject"}, Bucket: "test-bucket", Prefix: "allowed/"},
		{Effect: "deny", Actions: []string{"s3:DeleteObject"}, Bucket: "test-bucket", Prefix: "allowed/secret/"},
		{Effect: "allow", Actions: []string{"s3:GetObject"}, Bucket: "test-bucket", Prefix: "*"},
	})
	store.resetPolicyReads()

	keys := []string{
		"allowed/file.txt",
		"allowed/secret/file.txt",
		"private/file.txt",
		"allowedx/file.txt",
	}
	wantAllowed := []bool{true, false, false, false}

	errs := svc.AuthorizeBatch("batch-user", ActionDeleteObject, "test-bucket", keys)
	if len(errs) != len(keys) {
		t.Fatalf("AuthorizeBatch returned %d results, want %d", len(errs), len(keys))
	}
	for i, key := range keys {
		wantErr := error(nil)
		if !wantAllowed[i] {
			wantErr = ErrAccessDenied
		}
		if !errors.Is(errs[i], wantErr) {
			t.Fatalf("key %q batch error = %v, want %v", key, errs[i], wantErr)
		}
		// The batch result must agree with the single-key path for the same target.
		singleErr := svc.Authorize("batch-user", RequestTarget{
			Action: ActionDeleteObject,
			Bucket: "test-bucket",
			Key:    key,
		})
		if !errors.Is(singleErr, wantErr) {
			t.Fatalf("key %q single error = %v, want %v", key, singleErr, wantErr)
		}
	}
	if got := store.policyReadCount(); got != int64(len(keys)+1) {
		t.Fatalf("policy reads = %d, want %d (one batch read plus %d single-key checks)",
			got, len(keys)+1, len(keys))
	}

	// A different action must be evaluated independently of the delete allow.
	getErrs := svc.AuthorizeBatch("batch-user", ActionGetObject, "test-bucket", keys)
	for i, err := range getErrs {
		if err != nil {
			t.Fatalf("get key %q denied: %v", keys[i], err)
		}
	}
	// A different bucket must not match the delete statements.
	bucketErrs := svc.AuthorizeBatch("batch-user", ActionDeleteObject, "other-bucket", keys)
	for i, err := range bucketErrs {
		if !errors.Is(err, ErrAccessDenied) {
			t.Fatalf("other bucket key %q error = %v, want ErrAccessDenied", keys[i], err)
		}
	}
}

func TestAuthorizeBatchDeniesWhenPolicyMissing(t *testing.T) {
	svc, store := newBatchAuthService(t, true)

	errs := svc.AuthorizeBatch("missing-user", ActionDeleteObject, "test-bucket", []string{"a", "b"})
	if len(errs) != 2 {
		t.Fatalf("AuthorizeBatch returned %d results, want 2", len(errs))
	}
	for i, err := range errs {
		if !errors.Is(err, ErrAccessDenied) {
			t.Fatalf("result %d error = %v, want ErrAccessDenied", i, err)
		}
	}
	if got := store.policyReadCount(); got != 1 {
		t.Fatalf("policy reads = %d, want 1", got)
	}
}

func TestAuthorizeBatchEmptyInputSkipsPolicyRead(t *testing.T) {
	svc, store := newBatchAuthService(t, true)

	if errs := svc.AuthorizeBatch("batch-user", ActionDeleteObject, "test-bucket", nil); len(errs) != 0 {
		t.Fatalf("AuthorizeBatch returned %d results, want 0", len(errs))
	}
	if errs := svc.AuthorizeBatch("", ActionDeleteObject, "test-bucket", nil); len(errs) != 0 {
		t.Fatalf("AuthorizeBatch returned %d results, want 0", len(errs))
	}
	if got := store.policyReadCount(); got != 0 {
		t.Fatalf("policy reads = %d, want 0", got)
	}
}

func TestAuthorizeBatchDisabledAllowsWithoutPolicyRead(t *testing.T) {
	svc, store := newBatchAuthService(t, false)

	keys := []string{"a", "b", "c"}
	for _, err := range svc.AuthorizeBatch("any-user", ActionDeleteObject, "test-bucket", keys) {
		if err != nil {
			t.Fatalf("disabled auth must allow all keys, got %v", err)
		}
	}
	if got := store.policyReadCount(); got != 0 {
		t.Fatalf("policy reads = %d, want 0 when auth is disabled", got)
	}
}

// BenchmarkAuthorizeBatchMultiDelete compares the old per-key authorization
// path with batch authorization at multi-object delete sizes. The policy read
// count is reported per operation: it scales with key count before, and stays
// at one after.
func BenchmarkAuthorizeBatchMultiDelete(b *testing.B) {
	svc, store := newBatchAuthService(b, true)
	createBatchPolicyUser(b, svc, []models.AuthPolicyStatement{
		{Effect: "allow", Actions: []string{"s3:DeleteObject"}, Bucket: "test-bucket", Prefix: "allowed/"},
	})

	for _, count := range []int{1, 100, 1000} {
		keys := make([]string, count)
		for i := range keys {
			keys[i] = fmt.Sprintf("allowed/file-%04d.txt", i)
		}

		b.Run(fmt.Sprintf("per_key/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			store.resetPolicyReads()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, key := range keys {
					if err := svc.Authorize("batch-user", RequestTarget{
						Action: ActionDeleteObject,
						Bucket: "test-bucket",
						Key:    key,
					}); err != nil {
						b.Fatalf("authorize %q: %v", key, err)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(store.policyReadCount())/float64(b.N), "policy_reads/op")
		})

		b.Run(fmt.Sprintf("batch/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			store.resetPolicyReads()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, err := range svc.AuthorizeBatch("batch-user", ActionDeleteObject, "test-bucket", keys) {
					if err != nil {
						b.Fatalf("authorize batch: %v", err)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(store.policyReadCount())/float64(b.N), "policy_reads/op")
		})
	}
}
