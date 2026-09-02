package locks

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// The SQL lock keeps a leader election record in one row of a plain table, read by
// primary key and updated in place. It exists because the alternative when the
// controller's store is SQL-backed, a coordination.k8s.io Lease living in a
// versioned object store, is a poor fit: a lock is rewritten on every renew, and an
// append-only store turns that into hundreds of row versions of one object that
// every read has to sort through. Here a read is a point lookup and a renew is a
// single-row write, so the cost does not grow with the renew period, the
// compaction interval, or the number of replicas polling it.
const (
	sqlCreateTable = `CREATE TABLE IF NOT EXISTS leader_lock (
    name    TEXT    PRIMARY KEY,
    record  TEXT    NOT NULL,
    version INTEGER NOT NULL
)`
	// sqlProbe succeeds only if the table exists; used to tell a lost creation race
	// from a real failure.
	sqlProbe  = `SELECT 1 FROM leader_lock WHERE 1 = 0`
	sqlGet    = `SELECT record, version FROM leader_lock WHERE name = $1`
	sqlCreate = `INSERT INTO leader_lock (name, record, version) VALUES ($1, $2, 1) ON CONFLICT (name) DO NOTHING`
	sqlUpdate = `UPDATE leader_lock SET record = $1, version = version + 1 WHERE name = $2 AND version = $3`

	// legacyTakeoverGrace is how long the legacy lock must stay free before this lock
	// claims the election. Replicas still running the legacy lock poll every two
	// seconds and take a released lease on their next poll, so anything longer than
	// one poll lets them win, which is the point: an old-version replica that wins
	// keeps the election on one lock until it too is replaced.
	legacyTakeoverGrace = 5 * time.Second
)

// sqlGroupResource is what NotFound, AlreadyExists and Conflict errors report. There
// is no API group behind this table; client-go only inspects an error's reason.
var sqlGroupResource = schema.GroupResource{Group: "nah.obot.ai", Resource: "leaderlocks"}

// SQLOption adjusts a lock returned by NewSQL.
type SQLOption func(*sqlLock)

// WithLegacyLock makes the SQL lock defer to legacy, the lock an earlier version of
// the program used for the same election, until the first row exists in the
// leader_lock table. It is for the release that switches an election from another
// lock to this one: during that rolling update old and new replicas run side by side,
// and without this they would each elect a leader on their own lock.
//
// While the table has no row, Get reports the legacy record, so a replica on this
// lock is an ordinary follower behind a leader on the legacy lock. When client-go
// finds the legacy record free (released, or not renewed for a full lease duration)
// and calls Update, the SQL lock re-reads the legacy lock, backs off if a replica
// on the legacy lock has re-acquired it, waits legacyTakeoverGrace so that such
// replicas win any race, and only then creates the row. From then on the legacy lock
// is never read again. Remove the option once every replica runs the SQL lock.
func WithLegacyLock(legacy resourcelock.Interface) SQLOption {
	return func(l *sqlLock) { l.legacy = legacy }
}

type sqlLock struct {
	db       *sql.DB
	name     string
	identity string

	// version is the row version last observed by Get or written by Create or
	// Update. Zero means neither has happened and Update must refuse to run.
	version int64

	// legacy, when set, is consulted while the table has no row. See WithLegacyLock.
	legacy resourcelock.Interface
	// observedLegacy records that the last Get answered from the legacy lock, which
	// is the one case where Update may run with version zero.
	observedLegacy bool
	// legacyFreeSince is when this lock first saw the legacy lock free. Zero while it
	// is held.
	legacyFreeSince time.Time
	grace           time.Duration
	now             func() time.Time
}

// NewSQL returns a lock for the election called name, held under identity, backed by
// a row in the leader_lock table of db. It creates the table if it is missing.
//
// Like client-go's LeaseLock and the file lock in this package it is stateful: Get and
// Create remember the row version they observed, and Update succeeds only if that
// version is still current. That check is what stops two replicas from both
// believing they hold the lock. Use one lock per elector, per process.
func NewSQL(ctx context.Context, db *sql.DB, name, identity string, opts ...SQLOption) (resourcelock.Interface, error) {
	if db == nil {
		return nil, errors.New("locks: sql lock requires a database")
	}
	if name == "" {
		return nil, errors.New("locks: sql lock requires a name")
	}
	if identity == "" {
		return nil, errors.New("locks: sql lock requires an identity")
	}

	if _, err := db.ExecContext(ctx, sqlCreateTable); err != nil {
		// Two replicas starting on a fresh database at the same instant race here.
		// CREATE TABLE IF NOT EXISTS is not atomic in Postgres, and the loser gets a
		// unique violation on pg_type even though the table now exists. If the table
		// is there, the other replica won and there is nothing to do.
		if _, probe := db.ExecContext(ctx, sqlProbe); probe != nil {
			return nil, fmt.Errorf("locks: creating leader_lock table: %w", err)
		}
	}

	l := &sqlLock{db: db, name: name, identity: identity, grace: legacyTakeoverGrace, now: time.Now}
	for _, opt := range opts {
		opt(l)
	}
	return l, nil
}

// Get returns the current record. A missing row is reported as a Kubernetes NotFound
// because that is the only shape client-go's elector reads as "no lock yet, try
// Create". With a legacy lock configured, a missing row defers to that lock's record
// instead, so a leader on the legacy lock is respected.
func (l *sqlLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	var (
		raw     []byte
		version int64
	)
	err := l.db.QueryRowContext(ctx, sqlGet, l.name).Scan(&raw, &version)
	if errors.Is(err, sql.ErrNoRows) {
		if l.legacy != nil {
			return l.getLegacy(ctx)
		}
		return nil, nil, apierrors.NewNotFound(sqlGroupResource, l.name)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("locks: reading %s: %w", l.name, err)
	}

	var record resourcelock.LeaderElectionRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, nil, fmt.Errorf("locks: decoding %s: %w", l.name, err)
	}

	l.version = version
	l.observedLegacy = false
	l.legacyFreeSince = time.Time{}
	return &record, raw, nil
}

// getLegacy answers a Get from the legacy lock. The raw bytes are passed through
// unchanged because client-go compares them between polls to notice a renew.
func (l *sqlLock) getLegacy(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	record, raw, err := l.legacy.Get(ctx)
	if apierrors.IsNotFound(err) {
		l.observedLegacy = false
		return nil, nil, apierrors.NewNotFound(sqlGroupResource, l.name)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("locks: reading the legacy lock for %s: %w", l.name, err)
	}
	l.version = 0
	l.observedLegacy = true
	return record, raw, nil
}

// Create inserts the record. It fails with AlreadyExists if the row is present;
// client-go treats that as a lost race and tries again on its next period.
func (l *sqlLock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	raw, err := json.Marshal(ler)
	if err != nil {
		return fmt.Errorf("locks: encoding %s: %w", l.name, err)
	}

	result, err := l.db.ExecContext(ctx, sqlCreate, l.name, string(raw))
	if err != nil {
		return fmt.Errorf("locks: creating %s: %w", l.name, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("locks: creating %s: %w", l.name, err)
	}
	if n == 0 {
		return apierrors.NewAlreadyExists(sqlGroupResource, l.name)
	}

	l.version = 1
	l.observedLegacy = false
	l.legacyFreeSince = time.Time{}
	return nil
}

// Update replaces the record, but only while the row is still at the version this
// lock last observed. If another replica has written since, no row matches and this
// returns Conflict, which client-go answers by re-reading on its slow path.
//
// If the last Get was answered from the legacy lock there is no row to update;
// client-go is asking because the legacy record looked free, and the request becomes
// a takeover from the legacy lock.
func (l *sqlLock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	if l.version == 0 {
		if l.observedLegacy {
			return l.takeOverFromLegacy(ctx, ler)
		}
		return errors.New("locks: sql lock not initialized, call Get or Create first")
	}

	raw, err := json.Marshal(ler)
	if err != nil {
		return fmt.Errorf("locks: encoding %s: %w", l.name, err)
	}

	result, err := l.db.ExecContext(ctx, sqlUpdate, string(raw), l.name, l.version)
	if err != nil {
		return fmt.Errorf("locks: updating %s: %w", l.name, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("locks: updating %s: %w", l.name, err)
	}
	if n == 0 {
		return apierrors.NewConflict(sqlGroupResource, l.name,
			fmt.Errorf("version %d is no longer current", l.version))
	}

	l.version++
	return nil
}

// takeOverFromLegacy creates the first row once the legacy lock has been free for the
// grace period. Every refusal is a Conflict, which client-go logs and retries on its
// next poll, so the takeover is spread over a few polls rather than blocking one.
func (l *sqlLock) takeOverFromLegacy(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	record, _, err := l.legacy.Get(ctx)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("locks: reading the legacy lock for %s: %w", l.name, err)
	}
	now := l.now()
	if err == nil && l.heldByOther(record, now) {
		// A replica on the legacy lock took the election. Stay a follower.
		l.legacyFreeSince = time.Time{}
		return apierrors.NewConflict(sqlGroupResource, l.name,
			fmt.Errorf("the legacy lock is held by %s", record.HolderIdentity))
	}

	if l.legacyFreeSince.IsZero() {
		l.legacyFreeSince = now
	}
	if wait := l.grace - now.Sub(l.legacyFreeSince); wait > 0 {
		return apierrors.NewConflict(sqlGroupResource, l.name,
			fmt.Errorf("the legacy lock is free; waiting %s for replicas on the legacy lock to claim it first", wait.Round(time.Second)))
	}

	return l.Create(ctx, ler)
}

// heldByOther reports whether record names a live holder other than this lock's
// identity. A released record has an empty holder, and a holder that stopped
// renewing is treated as gone once its lease duration has passed.
func (l *sqlLock) heldByOther(record *resourcelock.LeaderElectionRecord, now time.Time) bool {
	if record.HolderIdentity == "" || record.HolderIdentity == l.identity {
		return false
	}
	expiry := record.RenewTime.Add(time.Duration(record.LeaseDurationSeconds) * time.Second)
	return expiry.After(now)
}

// RecordEvent is a no-op; there is no event stream behind a SQL table.
func (*sqlLock) RecordEvent(string) {}

// Identity returns the identity this lock acquires and renews under.
func (l *sqlLock) Identity() string { return l.identity }

// Describe names the lock for log lines.
func (l *sqlLock) Describe() string { return "leader_lock/" + l.name }
