package leader

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
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

func TestResourceLockSelectsByType(t *testing.T) {
	ctx := context.Background()

	// File lock: the existing path, unchanged.
	file := filepath.Join(t.TempDir(), "lock")
	rl, err := NewFileElectionConfig(file).resourceLock(ctx, "pod-1")
	if err != nil {
		t.Fatalf("file lock: %v", err)
	}
	if rl.Identity() != "pod-1" || rl.Describe() != file {
		t.Fatalf("file lock: identity %q describe %q", rl.Identity(), rl.Describe())
	}

	// SQL lock: a row in leader_lock, identity carried through from the elector.
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "lock.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ec := NewSQLElectionConfig("election", db)
	if ec.ResourceLockType != SQLLockType || ec.TTL != defaultElectionTTL() {
		t.Fatalf("NewSQLElectionConfig: type %q ttl %v", ec.ResourceLockType, ec.TTL)
	}
	rl, err = ec.resourceLock(ctx, "pod-2")
	if err != nil {
		t.Fatalf("sql lock: %v", err)
	}
	if rl.Identity() != "pod-2" || rl.Describe() != "leader_lock/election" {
		t.Fatalf("sql lock: identity %q describe %q", rl.Identity(), rl.Describe())
	}

	// A SQL config without a database must fail at lock construction, not later.
	if _, err := (&ElectionConfig{Name: "x", ResourceLockType: SQLLockType}).resourceLock(ctx, "pod-3"); err == nil {
		t.Fatal("expected an error for a sql lock with no database")
	}
}
