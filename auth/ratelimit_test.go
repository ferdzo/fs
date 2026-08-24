package auth

import (
	"testing"
	"time"
)

func TestFailureLimiterAllowsUnderLimit(t *testing.T) {
	f := newFailureLimiter(3, time.Minute)
	for i := 0; i < 3; i++ {
		if !f.allow("1.2.3.4") {
			t.Fatalf("attempt %d denied, want allowed", i+1)
		}
	}
	if f.allow("1.2.3.4") {
		t.Fatal("attempt 4 allowed, want denied")
	}
}

func TestFailureLimiterKeysAreIndependent(t *testing.T) {
	f := newFailureLimiter(1, time.Minute)
	if !f.allow("a") {
		t.Fatal("key a denied")
	}
	if !f.allow("b") {
		t.Fatal("key b denied though only key a failed")
	}
	if f.allow("a") {
		t.Fatal("key a allowed twice")
	}
}

func TestFailureLimiterWindowExpires(t *testing.T) {
	f := newFailureLimiter(1, 30*time.Millisecond)
	if !f.allow("k") {
		t.Fatal("first attempt denied")
	}
	if f.allow("k") {
		t.Fatal("second immediate attempt allowed")
	}
	time.Sleep(45 * time.Millisecond)
	if !f.allow("k") {
		t.Fatal("attempt after window expiry denied")
	}
}

func TestNilLimiterAllowsEverything(t *testing.T) {
	var f *failureLimiter
	for i := 0; i < 100; i++ {
		if !f.allow("x") {
			t.Fatal("nil limiter denied a request")
		}
	}
}
