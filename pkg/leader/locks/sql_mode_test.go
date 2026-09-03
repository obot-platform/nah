package locks

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/obot-platform/nah/pkg/log"
)

// captureInfof collects what the package logs at info level for the duration of a test.
func captureInfof(t *testing.T) func() []string {
	t.Helper()
	var (
		mu    sync.Mutex
		lines []string
	)
	restore := log.Infof
	log.Infof = func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, strings.TrimSpace(fmt.Sprintf(format, args...)))
	}
	t.Cleanup(func() { log.Infof = restore })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), lines...)
	}
}

func hasLine(lines []string, substr string) bool {
	for _, l := range lines {
		if strings.Contains(l, substr) {
			return true
		}
	}
	return false
}

func TestSQLReportsThatItDefersToTheLegacyLock(t *testing.T) {
	lines := captureInfof(t)
	legacy := &fakeLegacy{}
	legacy.held("old-1", time.Now())

	if _, err := NewSQL(context.Background(), newDB(t), "election", "new-1", WithLegacyLock(legacy)); err != nil {
		t.Fatal(err)
	}

	got := lines()
	if !hasLine(got, "has no row; deferring to the legacy lock") {
		t.Fatalf("expected a line saying the lock defers, got %q", got)
	}
	if hasLine(got, "will not be read") {
		t.Fatalf("the lock claimed it would not read the legacy lock although there is no row: %q", got)
	}
}

func TestSQLReportsThatTheLegacyLockIsNoLongerRead(t *testing.T) {
	// A replica that joins after the migration finished must not claim it is
	// deferring to anything. The row already decides the election.
	ctx := context.Background()
	db := newDB(t)
	first, err := NewSQL(ctx, db, "election", "new-1", WithLegacyLock(&fakeLegacy{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Create(ctx, record("new-1")); err != nil {
		t.Fatal(err)
	}

	lines := captureInfof(t)
	if _, err := NewSQL(ctx, db, "election", "new-2", WithLegacyLock(&fakeLegacy{})); err != nil {
		t.Fatal(err)
	}

	got := lines()
	if !hasLine(got, "already has a row") || !hasLine(got, "will not be read") {
		t.Fatalf("expected a line saying the legacy lock will not be read, got %q", got)
	}
	if hasLine(got, "deferring") {
		t.Fatalf("the lock claimed it defers although a row exists: %q", got)
	}
}

func TestSQLWithoutALegacyLockReportsNothingAboutOne(t *testing.T) {
	lines := captureInfof(t)
	if _, err := NewSQL(context.Background(), newDB(t), "election", "solo"); err != nil {
		t.Fatal(err)
	}
	if got := lines(); hasLine(got, "legacy") {
		t.Fatalf("a lock with no legacy lock mentioned one: %q", got)
	}
}

func TestSQLModeIsReportedPerElection(t *testing.T) {
	// Two elections share a table. One has a row, the other does not, and each must
	// report its own state.
	ctx := context.Background()
	db := newDB(t)
	a, err := NewSQL(ctx, db, "election-a", "id", WithLegacyLock(&fakeLegacy{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Create(ctx, record("id")); err != nil {
		t.Fatal(err)
	}

	lines := captureInfof(t)
	if _, err := NewSQL(ctx, db, "election-a", "id2", WithLegacyLock(&fakeLegacy{})); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSQL(ctx, db, "election-b", "id3", WithLegacyLock(&fakeLegacy{})); err != nil {
		t.Fatal(err)
	}

	got := lines()
	if !hasLine(got, "leader_lock/election-a already has a row") {
		t.Fatalf("election-a should report that it has a row: %q", got)
	}
	if !hasLine(got, "leader_lock/election-b has no row") {
		t.Fatalf("election-b should report that it has no row: %q", got)
	}
}
