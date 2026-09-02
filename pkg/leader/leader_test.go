package leader

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/obot-platform/nah/pkg/leader/locks"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func TestJSONClientConfigForcesJSONContentNegotiation(t *testing.T) {
	original := &rest.Config{
		Host: "https://example.invalid",
		ContentConfig: rest.ContentConfig{
			AcceptContentTypes: "application/vnd.kubernetes.protobuf,application/json",
			ContentType:        "application/vnd.kubernetes.protobuf",
		},
	}

	cfg := jsonClientConfig(original)

	if cfg == original {
		t.Fatal("expected a copied config")
	}
	if cfg.AcceptContentTypes != runtime.ContentTypeJSON {
		t.Fatalf("expected accept content types %q, got %q", runtime.ContentTypeJSON, cfg.AcceptContentTypes)
	}
	if cfg.ContentType != runtime.ContentTypeJSON {
		t.Fatalf("expected content type %q, got %q", runtime.ContentTypeJSON, cfg.ContentType)
	}
	if original.AcceptContentTypes != "application/vnd.kubernetes.protobuf,application/json" {
		t.Fatalf("expected original accept content types to remain unchanged, got %q", original.AcceptContentTypes)
	}
	if original.ContentType != "application/vnd.kubernetes.protobuf" {
		t.Fatalf("expected original content type to remain unchanged, got %q", original.ContentType)
	}
}

func TestResourceLockPrefersNewLock(t *testing.T) {
	want := locks.NewFile("unused", filepath.Join(t.TempDir(), "lock"))

	var gotIdentity string
	ec := NewElectionConfigWithLock("election", func(identity string) (resourcelock.Interface, error) {
		gotIdentity = identity
		return want, nil
	})

	got, err := ec.resourceLock("pod-1")
	if err != nil {
		t.Fatalf("resourceLock: %v", err)
	}
	if got != want {
		t.Fatal("expected the lock returned by NewLock to be used as-is")
	}
	// The elector reports whoever the lock says holds it, so the identity handed to
	// NewLock has to be the one nah is electing under.
	if gotIdentity != "pod-1" {
		t.Fatalf("NewLock received identity %q, want %q", gotIdentity, "pod-1")
	}
}

func TestResourceLockPropagatesNewLockError(t *testing.T) {
	ec := NewElectionConfigWithLock("election", func(string) (resourcelock.Interface, error) {
		return nil, errors.New("no database")
	})

	if _, err := ec.resourceLock("pod-1"); err == nil || !strings.Contains(err.Error(), "no database") {
		t.Fatalf("expected NewLock error to propagate, got %v", err)
	}
}

// Existing callers set ResourceLockType and never NewLock; that path must be untouched.
func TestResourceLockFallsBackToTypeWhenNewLockUnset(t *testing.T) {
	file := filepath.Join(t.TempDir(), "lock")
	ec := NewFileElectionConfig(file)

	rl, err := ec.resourceLock("pod-1")
	if err != nil {
		t.Fatalf("resourceLock: %v", err)
	}
	if rl.Identity() != "pod-1" {
		t.Fatalf("Identity() = %q, want %q", rl.Identity(), "pod-1")
	}
	if rl.Describe() != file {
		t.Fatalf("Describe() = %q, want the file path %q", rl.Describe(), file)
	}
}

func TestNewElectionConfigWithLockKeepsDefaultTiming(t *testing.T) {
	ec := NewElectionConfigWithLock("election", func(string) (resourcelock.Interface, error) {
		return nil, nil
	})

	// Only the lock storage changes; TTL and everything derived from it stay put.
	if ec.TTL != defaultElectionTTL() {
		t.Fatalf("TTL = %v, want default %v", ec.TTL, defaultElectionTTL())
	}
	if ec.Name != "election" {
		t.Fatalf("Name = %q, want %q", ec.Name, "election")
	}
	if ec.NewLock == nil {
		t.Fatal("NewLock must be set")
	}
}
