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

// bridge is a SQL lock with a fake legacy lock, a controllable clock, and a recorded
// sleep. Sleeping advances the clock and runs onSleep, which lets a test change the
// legacy lock "during" the grace period.
type bridge struct {
	l       *sqlLock
	legacy  *fakeLegacy
	clock   time.Time
	slept   []time.Duration
	onSleep func()
}

func newBridge(t *testing.T, identity string) *bridge {
	t.Helper()
	b := &bridge{legacy: &fakeLegacy{}, clock: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)}
	lock, err := NewSQL(context.Background(), newDB(t), "election", identity, WithLegacyLock(b.legacy))
	if err != nil {
		t.Fatal(err)
	}
	b.l = lock.(*sqlLock)
	b.l.grace = 5 * time.Second
	b.l.now = func() time.Time { return b.clock }
	b.l.sleep = func(_ context.Context, d time.Duration) error {
		b.slept = append(b.slept, d)
		b.clock = b.clock.Add(d)
		if b.onSleep != nil {
			b.onSleep()
		}
		return nil
	}
	return b
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
	b := newBridge(t, "new-1")
	b.legacy.held("old-1", b.clock)

	got, _, err := b.l.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.HolderIdentity != "old-1" {
		t.Fatalf("Get should report the legacy holder, got %q", got.HolderIdentity)
	}

	// client-go would not call Update while the holder is live, but if it did the
	// lock must still refuse rather than create a second election.
	err = b.l.Update(ctx, record("new-1"))
	if !apierrors.IsConflict(err) {
		t.Fatalf("Update against a held legacy lock should be Conflict, got %v", err)
	}
	if rowCount(t, b.l) != 0 {
		t.Fatal("a row was created while the legacy lock is held")
	}
	if len(b.slept) != 0 {
		t.Fatal("no grace period should run while the legacy lock is held")
	}
}

func TestSQLLegacyReleasedLeaseIsTakenOverAfterGrace(t *testing.T) {
	ctx := context.Background()
	b := newBridge(t, "new-1")
	b.legacy.released(b.clock)

	if _, _, err := b.l.Get(ctx); err != nil {
		t.Fatalf("Get: %v", err)
	}
	// One Update: wait the grace period, re-check, create.
	if err := b.l.Update(ctx, record("new-1")); err != nil {
		t.Fatalf("Update after a release should take over, got %v", err)
	}
	if len(b.slept) != 1 || b.slept[0] != 5*time.Second {
		t.Fatalf("expected one full grace period, slept %v", b.slept)
	}
	if rowCount(t, b.l) != 1 {
		t.Fatal("expected exactly one row after takeover")
	}

	// From here on the SQL row is authoritative and the legacy lock is not read.
	gets := b.legacy.gets
	got, _, err := b.l.Get(ctx)
	if err != nil || got.HolderIdentity != "new-1" {
		t.Fatalf("Get after takeover: holder %q err %v", got.HolderIdentity, err)
	}
	if err := b.l.Update(ctx, record("new-1")); err != nil {
		t.Fatalf("renew after takeover: %v", err)
	}
	if b.legacy.gets != gets {
		t.Fatal("legacy lock was read after the row existed")
	}
}

func TestSQLLegacyClaimedDuringGraceBacksOff(t *testing.T) {
	ctx := context.Background()
	b := newBridge(t, "new-1")
	b.legacy.released(b.clock)

	// A replica still on the legacy lock takes the released lease during the grace
	// period, as it would on its next two second poll.
	b.onSleep = func() { b.legacy.held("old-2", b.clock) }
	if _, _, err := b.l.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.l.Update(ctx, record("new-1")); !apierrors.IsConflict(err) {
		t.Fatalf("expected Conflict once the legacy lock is claimed during the grace period, got %v", err)
	}
	if rowCount(t, b.l) != 0 {
		t.Fatal("row created while an old replica holds the legacy lock")
	}

	// old-2 leads for a while; client-go only calls Get and sees a live holder.
	b.onSleep = nil
	for i := 0; i < 20; i++ {
		b.clock = b.clock.Add(2 * time.Second)
		b.legacy.held("old-2", b.clock)
		got, _, err := b.l.Get(ctx)
		if err != nil || got.HolderIdentity != "old-2" {
			t.Fatalf("Get during old-2's term: holder %q err %v", got.HolderIdentity, err)
		}
	}

	// When old-2 releases in turn, the takeover waits a full grace period again.
	b.clock = b.clock.Add(time.Second)
	b.legacy.released(b.clock)
	if _, _, err := b.l.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.l.Update(ctx, record("new-1")); err != nil {
		t.Fatalf("takeover after the second release: %v", err)
	}
	if len(b.slept) != 2 || b.slept[1] != 5*time.Second {
		t.Fatalf("the second takeover must wait a full grace period, slept %v", b.slept)
	}
	if rowCount(t, b.l) != 1 {
		t.Fatal("expected one row after the final takeover")
	}
}

func TestSQLLegacyExpiredHolderIsTakenOver(t *testing.T) {
	ctx := context.Background()
	b := newBridge(t, "new-1")
	// The holder crashed: the record still names it but has not been renewed for
	// longer than its lease duration.
	b.legacy.held("old-1", b.clock.Add(-2*time.Minute))

	if _, _, err := b.l.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.l.Update(ctx, record("new-1")); err != nil {
		t.Fatalf("expected takeover from an expired legacy holder, got %v", err)
	}
	if len(b.slept) != 1 {
		t.Fatalf("expected one grace period, slept %v", b.slept)
	}
	if rowCount(t, b.l) != 1 {
		t.Fatal("expected one row")
	}
}

func TestSQLLegacyOwnIdentityIsNotAnObstacle(t *testing.T) {
	// A container restart keeps the pod name, so the legacy record can name this very
	// identity. That must count as free, the same way client-go treats its own lease.
	ctx := context.Background()
	b := newBridge(t, "same-pod")
	b.legacy.held("same-pod", b.clock)

	if _, _, err := b.l.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.l.Update(ctx, record("same-pod")); err != nil {
		t.Fatalf("expected takeover, got %v", err)
	}
	if rowCount(t, b.l) != 1 {
		t.Fatal("expected one row")
	}
}

func TestSQLLegacyShutdownDuringGraceAbortsTakeover(t *testing.T) {
	ctx := context.Background()
	b := newBridge(t, "new-1")
	b.legacy.released(b.clock)
	b.l.sleep = func(context.Context, time.Duration) error { return context.Canceled }

	if _, _, err := b.l.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.l.Update(ctx, record("new-1")); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the context error, got %v", err)
	}
	if rowCount(t, b.l) != 0 {
		t.Fatal("row created although the process was shutting down")
	}
}

func TestSQLLegacyMissingBehavesLikeNoLegacy(t *testing.T) {
	ctx := context.Background()
	b := newBridge(t, "new-1")
	b.legacy.notFound()

	_, _, err := b.l.Get(ctx)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("with no legacy record Get must be NotFound so client-go calls Create, got %v", err)
	}
	if err := b.l.Create(ctx, record("new-1")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if rowCount(t, b.l) != 1 {
		t.Fatal("expected one row")
	}
}

func TestSQLLegacyIsIgnoredOnceARowExists(t *testing.T) {
	ctx := context.Background()
	b := newBridge(t, "new-1")
	if err := b.l.Create(ctx, record("new-1")); err != nil {
		t.Fatal(err)
	}
	b.legacy.held("old-1", b.clock) // a stale old replica still renewing its own lease

	got, _, err := b.l.Get(ctx)
	if err != nil || got.HolderIdentity != "new-1" {
		t.Fatalf("Get must answer from the row once it exists: holder %q err %v", got.HolderIdentity, err)
	}
	if err := b.l.Update(ctx, record("new-1")); err != nil {
		t.Fatalf("Update must run normally once the row exists: %v", err)
	}
	if b.legacy.gets != 0 {
		t.Fatal("legacy lock was read although a row exists")
	}
}

func TestSQLLegacyReadErrorIsReturned(t *testing.T) {
	ctx := context.Background()
	b := newBridge(t, "new-1")
	b.legacy.err = errors.New("api unreachable")

	if _, _, err := b.l.Get(ctx); err == nil || apierrors.IsNotFound(err) {
		t.Fatalf("a legacy read failure must surface as an error, not NotFound, got %v", err)
	}
	if rowCount(t, b.l) != 0 {
		t.Fatal("row created despite a legacy read failure")
	}
}

func TestSQLTwoNewReplicasTakeOverOnce(t *testing.T) {
	// Both new replicas see the same release and both wait the grace period; only
	// one row can be created and the other must see AlreadyExists, which client-go
	// treats as a lost race.
	ctx := context.Background()
	a := newBridge(t, "new-a")
	bl := &sqlLock{db: a.l.db, name: a.l.name, identity: "new-b", legacy: a.legacy, grace: a.l.grace, now: a.l.now, sleep: a.l.sleep}
	a.legacy.released(a.clock)

	for _, l := range []*sqlLock{a.l, bl} {
		if _, _, err := l.Get(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.l.Update(ctx, record("new-a")); err != nil {
		t.Fatalf("new-a takeover: %v", err)
	}
	err := bl.Update(ctx, record("new-b"))
	if !apierrors.IsAlreadyExists(err) {
		t.Fatalf("new-b must lose the race with AlreadyExists, got %v", err)
	}
	got, _, err := bl.Get(ctx)
	if err != nil || got.HolderIdentity != "new-a" {
		t.Fatalf("new-b must now follow new-a: holder %q err %v", got.HolderIdentity, err)
	}
	if rowCount(t, a.l) != 1 {
		t.Fatal("expected exactly one row")
	}
}
