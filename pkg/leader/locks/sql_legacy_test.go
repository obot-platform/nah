package locks

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// fakeLegacy stands in for the Lease lock an earlier version of a program used. Tests
// set what its Get answers; Create and Update are never expected to be called.
type fakeLegacy struct {
	record *resourcelock.LeaderElectionRecord
	err    error
	gets   int
}

func (f *fakeLegacy) Get(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	f.gets++
	if f.err != nil {
		return nil, nil, f.err
	}
	raw, _ := json.Marshal(f.record)
	return f.record, raw, nil
}

func (f *fakeLegacy) Create(context.Context, resourcelock.LeaderElectionRecord) error {
	return errors.New("legacy Create must not be called")
}

func (f *fakeLegacy) Update(context.Context, resourcelock.LeaderElectionRecord) error {
	return errors.New("legacy Update must not be called")
}

func (*fakeLegacy) RecordEvent(string) {}
func (*fakeLegacy) Identity() string   { return "legacy" }
func (*fakeLegacy) Describe() string   { return "fake legacy lock" }
func (f *fakeLegacy) notFound()        { f.record, f.err = nil, apierrors.NewNotFound(sqlGroupResource, "x") }
func (f *fakeLegacy) held(holder string, renewedAt time.Time) {
	f.err = nil
	f.record = &resourcelock.LeaderElectionRecord{
		HolderIdentity:       holder,
		LeaseDurationSeconds: 60,
		AcquireTime:          metav1.NewTime(renewedAt),
		RenewTime:            metav1.NewTime(renewedAt),
	}
}

// released mirrors what client-go writes when a leader shuts down cleanly.
func (f *fakeLegacy) released(at time.Time) {
	f.err = nil
	f.record = &resourcelock.LeaderElectionRecord{HolderIdentity: "", LeaseDurationSeconds: 1, RenewTime: metav1.NewTime(at)}
}

// newBridged returns a SQL lock with a fake legacy lock, a controllable clock, and a
// short grace period. The clock starts at a fixed instant that the tests advance.
func newBridged(t *testing.T, identity string) (*sqlLock, *fakeLegacy, *time.Time) {
	t.Helper()
	legacy := &fakeLegacy{}
	clock := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	lock, err := NewSQL(context.Background(), newDB(t), "election", identity, WithLegacyLock(legacy))
	if err != nil {
		t.Fatal(err)
	}
	l := lock.(*sqlLock)
	l.grace = 5 * time.Second
	l.now = func() time.Time { return clock }
	return l, legacy, &clock
}

func rowCount(t *testing.T, l *sqlLock) int {
	t.Helper()
	var n int
	if err := l.db.QueryRow(`SELECT count(*) FROM leader_lock`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSQLLegacyHeldLeaseIsReportedAndNotTakenOver(t *testing.T) {
	ctx := context.Background()
	l, legacy, clock := newBridged(t, "new-1")
	legacy.held("old-1", *clock)

	got, _, err := l.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.HolderIdentity != "old-1" {
		t.Fatalf("Get should report the legacy holder, got %q", got.HolderIdentity)
	}

	// client-go would not call Update while the holder is live, but if it did the
	// lock must still refuse rather than create a second election.
	err = l.Update(ctx, record("new-1"))
	if !apierrors.IsConflict(err) {
		t.Fatalf("Update against a held legacy lock should be Conflict, got %v", err)
	}
	if rowCount(t, l) != 0 {
		t.Fatal("a row was created while the legacy lock is held")
	}
}

func TestSQLLegacyReleasedLeaseIsTakenOverAfterGrace(t *testing.T) {
	ctx := context.Background()
	l, legacy, clock := newBridged(t, "new-1")
	legacy.released(*clock)

	if _, _, err := l.Get(ctx); err != nil {
		t.Fatalf("Get: %v", err)
	}
	// First poll after the release: the grace period starts, nothing is created.
	if err := l.Update(ctx, record("new-1")); !apierrors.IsConflict(err) {
		t.Fatalf("first Update should be Conflict while the grace period runs, got %v", err)
	}
	if rowCount(t, l) != 0 {
		t.Fatal("row created before the grace period passed")
	}

	*clock = clock.Add(2 * time.Second)
	if err := l.Update(ctx, record("new-1")); !apierrors.IsConflict(err) {
		t.Fatalf("Update inside the grace period should be Conflict, got %v", err)
	}

	*clock = clock.Add(4 * time.Second) // 6s since the release was first seen
	if err := l.Update(ctx, record("new-1")); err != nil {
		t.Fatalf("Update after the grace period should create the row, got %v", err)
	}
	if rowCount(t, l) != 1 {
		t.Fatal("expected exactly one row after takeover")
	}

	// From here on the SQL row is authoritative and the legacy lock is not read.
	gets := legacy.gets
	got, _, err := l.Get(ctx)
	if err != nil || got.HolderIdentity != "new-1" {
		t.Fatalf("Get after takeover: holder %q err %v", got.HolderIdentity, err)
	}
	if err := l.Update(ctx, record("new-1")); err != nil {
		t.Fatalf("renew after takeover: %v", err)
	}
	if legacy.gets != gets {
		t.Fatal("legacy lock was read after the row existed")
	}
}

func TestSQLLegacyReacquiredDuringGraceBacksOff(t *testing.T) {
	ctx := context.Background()
	l, legacy, clock := newBridged(t, "new-1")
	legacy.released(*clock)

	if _, _, err := l.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.Update(ctx, record("new-1")); !apierrors.IsConflict(err) {
		t.Fatalf("expected Conflict starting the grace period, got %v", err)
	}

	// A replica still on the legacy lock takes the released lease, as it would on
	// its next two second poll.
	*clock = clock.Add(2 * time.Second)
	legacy.held("old-2", *clock)
	if err := l.Update(ctx, record("new-1")); !apierrors.IsConflict(err) {
		t.Fatalf("expected Conflict once the legacy lock is held again, got %v", err)
	}
	*clock = clock.Add(10 * time.Second)
	legacy.held("old-2", *clock) // still renewing
	if err := l.Update(ctx, record("new-1")); !apierrors.IsConflict(err) {
		t.Fatalf("a renewing legacy holder must keep winning, got %v", err)
	}
	if rowCount(t, l) != 0 {
		t.Fatal("row created while an old replica holds the legacy lock")
	}

	// When that replica releases in turn, the grace period starts over.
	*clock = clock.Add(time.Second)
	legacy.released(*clock)
	if err := l.Update(ctx, record("new-1")); !apierrors.IsConflict(err) {
		t.Fatalf("grace period must restart after a re-acquire, got %v", err)
	}
	*clock = clock.Add(6 * time.Second)
	if err := l.Update(ctx, record("new-1")); err != nil {
		t.Fatalf("takeover after the second release: %v", err)
	}
	if rowCount(t, l) != 1 {
		t.Fatal("expected one row after the final takeover")
	}
}

func TestSQLLegacyExpiredHolderIsTakenOver(t *testing.T) {
	ctx := context.Background()
	l, legacy, clock := newBridged(t, "new-1")
	// The holder crashed: the record still names it but has not been renewed for
	// longer than its lease duration.
	legacy.held("old-1", clock.Add(-2*time.Minute))

	if _, _, err := l.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.Update(ctx, record("new-1")); !apierrors.IsConflict(err) {
		t.Fatalf("expected the grace period to start, got %v", err)
	}
	*clock = clock.Add(6 * time.Second)
	if err := l.Update(ctx, record("new-1")); err != nil {
		t.Fatalf("expected takeover from an expired legacy holder, got %v", err)
	}
	if rowCount(t, l) != 1 {
		t.Fatal("expected one row")
	}
}

func TestSQLLegacyOwnIdentityIsNotAnObstacle(t *testing.T) {
	// A container restart keeps the pod name, so the legacy record can name this very
	// identity. That must count as free, the same way client-go treats its own lease.
	ctx := context.Background()
	l, legacy, clock := newBridged(t, "same-pod")
	legacy.held("same-pod", *clock)

	if _, _, err := l.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.Update(ctx, record("same-pod")); !apierrors.IsConflict(err) {
		t.Fatalf("expected the grace period to start, got %v", err)
	}
	*clock = clock.Add(6 * time.Second)
	if err := l.Update(ctx, record("same-pod")); err != nil {
		t.Fatalf("expected takeover, got %v", err)
	}
}

func TestSQLLegacyMissingBehavesLikeNoLegacy(t *testing.T) {
	ctx := context.Background()
	l, legacy, _ := newBridged(t, "new-1")
	legacy.notFound()

	_, _, err := l.Get(ctx)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("with no legacy record Get must be NotFound so client-go calls Create, got %v", err)
	}
	if err := l.Create(ctx, record("new-1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if rowCount(t, l) != 1 {
		t.Fatal("expected one row")
	}
}

func TestSQLLegacyIsIgnoredOnceARowExists(t *testing.T) {
	ctx := context.Background()
	l, legacy, clock := newBridged(t, "new-1")
	if err := l.Create(ctx, record("new-1")); err != nil {
		t.Fatal(err)
	}
	legacy.held("old-1", *clock) // a stale old replica still renewing its own lease

	got, _, err := l.Get(ctx)
	if err != nil || got.HolderIdentity != "new-1" {
		t.Fatalf("Get must answer from the row once it exists: holder %q err %v", got.HolderIdentity, err)
	}
	if err := l.Update(ctx, record("new-1")); err != nil {
		t.Fatalf("Update must run normally once the row exists: %v", err)
	}
	if legacy.gets != 0 {
		t.Fatal("legacy lock was read although a row exists")
	}
}

func TestSQLLegacyReadErrorIsReturned(t *testing.T) {
	ctx := context.Background()
	l, legacy, _ := newBridged(t, "new-1")
	legacy.err = errors.New("api unreachable")

	if _, _, err := l.Get(ctx); err == nil || apierrors.IsNotFound(err) {
		t.Fatalf("a legacy read failure must surface as an error, not NotFound, got %v", err)
	}
	if rowCount(t, l) != 0 {
		t.Fatal("row created despite a legacy read failure")
	}
}

func TestSQLTwoNewReplicasTakeOverOnce(t *testing.T) {
	// Both new replicas see the same release; the grace period passes for both; only
	// one row can be created and the other must see AlreadyExists, which client-go
	// treats as a lost race.
	ctx := context.Background()
	a, legacyA, clockA := newBridged(t, "new-a")
	b := &sqlLock{db: a.db, name: a.name, identity: "new-b", legacy: legacyA, grace: a.grace, now: a.now}
	legacyA.released(*clockA)

	for _, l := range []*sqlLock{a, b} {
		if _, _, err := l.Get(ctx); err != nil {
			t.Fatal(err)
		}
		if err := l.Update(ctx, record(l.identity)); !apierrors.IsConflict(err) {
			t.Fatalf("%s: expected grace Conflict, got %v", l.identity, err)
		}
	}
	*clockA = clockA.Add(6 * time.Second)
	if err := a.Update(ctx, record("new-a")); err != nil {
		t.Fatalf("new-a takeover: %v", err)
	}
	err := b.Update(ctx, record("new-b"))
	if !apierrors.IsAlreadyExists(err) {
		t.Fatalf("new-b must lose the race with AlreadyExists, got %v", err)
	}
	got, _, err := b.Get(ctx)
	if err != nil || got.HolderIdentity != "new-a" {
		t.Fatalf("new-b must now follow new-a: holder %q err %v", got.HolderIdentity, err)
	}
	if rowCount(t, a) != 1 {
		t.Fatal("expected exactly one row")
	}
}
