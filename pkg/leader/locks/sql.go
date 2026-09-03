package locks

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/obot-platform/nah/pkg/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// The SQL lock keeps the leader election record in one row of a plain table. A Lease
// in a versioned object store is a poor fit for a lock: every renew appends a row
// version and every read sorts through them. Here a read is a point lookup and a
// renew is a single-row update, so the cost does not grow with renews or replicas.
const (
	sqlCreateTable = `CREATE TABLE IF NOT EXISTS leader_lock (
    name    TEXT    PRIMARY KEY,
    record  TEXT    NOT NULL,
    version INTEGER NOT NULL
)`
	// sqlProbe succeeds only if the table exists.
	sqlProbe  = `SELECT 1 FROM leader_lock WHERE 1 = 0`
	sqlExists = `SELECT EXISTS(SELECT 1 FROM leader_lock WHERE name = $1)`
	sqlGet    = `SELECT record, version FROM leader_lock WHERE name = $1`
	sqlCreate = `INSERT INTO leader_lock (name, record, version) VALUES ($1, $2, 1) ON CONFLICT (name) DO NOTHING`
	sqlUpdate = `UPDATE leader_lock SET record = $1, version = version + 1 WHERE name = $2 AND version = $3`

	// legacyTakeoverGrace is how long the legacy lock must stay free before this lock
	// claims the election. Replicas on the legacy lock poll every two seconds and take
	// a released lease on their next poll, so waiting longer than one poll lets them
	// win and keeps the election on one lock until they are gone.
	legacyTakeoverGrace = 5 * time.Second
)

// sqlGroupResource is what NotFound, AlreadyExists and Conflict errors report.
// client-go only inspects an error's reason.
var sqlGroupResource = schema.GroupResource{Group: "nah.obot.ai", Resource: "leaderlocks"}

// SQLOption adjusts a lock returned by NewSQL.
type SQLOption func(*sqlLock)

// WithLegacyLock makes the SQL lock defer to legacy, the lock an earlier release used
// for the same election, until the first row exists. While the table has no row, Get
// reports the legacy record, so a replica on this lock follows a leader on the legacy
// lock. When client-go finds the legacy record free and calls Update, the lock waits
// legacyTakeoverGrace, backs off if a legacy replica claimed the lock in the meantime,
// and otherwise creates the row. After that the legacy lock is never read. Remove the
// option once every replica runs the SQL lock.
func WithLegacyLock(legacy resourcelock.Interface) SQLOption {
	return func(l *sqlLock) { l.legacy = legacy }
}

type sqlLock struct {
	db       *sql.DB
	name     string
	identity string

	// mu guards the fields below. client-go calls from one goroutine, but the lock
	// does not rely on that.
	mu sync.Mutex

	// version is the row version last observed by Get or written by Create or
	// Update. Zero means neither has happened and Update must refuse to run.
	version int64

	// legacy, when set, is consulted while the table has no row. See WithLegacyLock.
	legacy resourcelock.Interface
	// observedLegacy is set when the last Get answered from the legacy lock, the one
	// case in which Update may run with version zero.
	observedLegacy bool
	// legacyHolderLogged is the legacy holder last logged, so it is logged once.
	legacyHolderLogged string
	grace              time.Duration
	now                func() time.Time
	sleep              func(context.Context, time.Duration) error
}

// NewSQL returns a lock for the election called name, held under identity, backed by
// a row in the leader_lock table of db. It creates the table if it is missing.
//
// The lock is stateful, like client-go's LeaseLock: Update succeeds only if the row is
// still at the version this lock last observed, which is what stops two replicas from
// both believing they hold it. Use one lock per elector.
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
		// CREATE TABLE IF NOT EXISTS is not atomic in Postgres. Two replicas starting
		// on a fresh database race here and the loser gets a unique violation even
		// though the table now exists, so only fail if the table is really missing.
		if _, probe := db.ExecContext(ctx, sqlProbe); probe != nil {
			return nil, fmt.Errorf("locks: creating leader_lock table: %w", err)
		}
	}

	l := &sqlLock{db: db, name: name, identity: identity, grace: legacyTakeoverGrace, now: time.Now, sleep: sleepContext}
	for _, opt := range opts {
		opt(l)
	}
	if l.legacy != nil {
		l.logLegacyMode(ctx)
	}
	return l, nil
}

// logLegacyMode reports whether this election will read the legacy lock, so that a
// migration still in progress can be told from one that has finished. A replica that
// joins after the row exists never reads the legacy lock, whatever its configuration
// says.
func (l *sqlLock) logLegacyMode(ctx context.Context) {
	var exists bool
	if err := l.db.QueryRowContext(ctx, sqlExists, l.name).Scan(&exists); err != nil {
		// Reporting the mode is not worth failing construction over. The lock logs
		// what it does at each transition anyway.
		return
	}
	if exists {
		log.Infof("%s already has a row; the legacy lock %s is configured but will not be read", l.Describe(), l.legacy.Describe())
		return
	}
	log.Infof("%s has no row; deferring to the legacy lock %s until one exists", l.Describe(), l.legacy.Describe())
}

// Get returns the current record. A missing row is reported as NotFound, which
// client-go reads as "no lock yet, try Create", unless a legacy lock is configured,
// in which case its record is reported instead.
func (l *sqlLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

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
	return &record, raw, nil
}

// getLegacy answers a Get from the legacy lock. The raw bytes pass through unchanged
// because client-go compares them between polls to notice a renew.
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
	if l.heldByOther(record, l.now()) {
		l.logFollowing(record.HolderIdentity)
	}
	return record, raw, nil
}

// logFollowing logs, once per holder, that this replica follows a legacy leader.
func (l *sqlLock) logFollowing(holder string) {
	if holder == l.legacyHolderLogged {
		return
	}
	l.legacyHolderLogged = holder
	log.Infof("%s has no row yet; following the legacy lock %s held by %s", l.Describe(), l.legacy.Describe(), holder)
}

// Create inserts the record. It fails with AlreadyExists if the row is present, which
// client-go treats as a lost race.
func (l *sqlLock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.create(ctx, ler)
}

// create is Create for callers that already hold mu.
func (l *sqlLock) create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
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
	return nil
}

// Update replaces the record if the row is still at the version this lock last
// observed, and returns Conflict otherwise. If the last Get was answered from the
// legacy lock there is no row yet, and the call becomes a takeover from that lock.
func (l *sqlLock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()

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

// takeOverFromLegacy creates the first row once the legacy lock has stayed free for
// the grace period. The wait happens inside this call, so client-go sees one Update
// and logs no failures. The caller holds mu.
func (l *sqlLock) takeOverFromLegacy(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	record, before, err := l.legacy.Get(ctx)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("locks: reading the legacy lock for %s: %w", l.name, err)
	}
	if err == nil && l.heldByOther(record, l.now()) {
		l.logFollowing(record.HolderIdentity)
		return apierrors.NewConflict(sqlGroupResource, l.name,
			fmt.Errorf("the legacy lock is held by %s", record.HolderIdentity))
	}

	log.Infof("%s has no row and the legacy lock %s is free; waiting %s for replicas on the legacy lock to claim it first",
		l.Describe(), l.legacy.Describe(), l.grace)
	if err := l.sleep(ctx, l.grace); err != nil {
		return err
	}

	// A live holder rewrites the record every two seconds, so a record that changed
	// during the wait and now names a holder was claimed. This deliberately ignores the
	// record's timestamps, which were written with another machine's clock.
	record, after, err := l.legacy.Get(ctx)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("locks: reading the legacy lock for %s: %w", l.name, err)
	}
	if err == nil && !bytes.Equal(before, after) && record.HolderIdentity != "" && record.HolderIdentity != l.identity {
		l.legacyHolderLogged = record.HolderIdentity
		log.Infof("%s claimed the legacy lock %s during the grace period; %s stays a follower",
			record.HolderIdentity, l.legacy.Describe(), l.identity)
		return apierrors.NewConflict(sqlGroupResource, l.name,
			fmt.Errorf("the legacy lock was claimed by %s", record.HolderIdentity))
	}

	if err := l.create(ctx, ler); err != nil {
		return err
	}
	log.Infof("%s took over the election from the legacy lock %s; the legacy lock is no longer read", l.Describe(), l.legacy.Describe())
	return nil
}

// heldByOther reports whether record names another live holder. A released record
// has an empty holder; a holder that stopped renewing counts as gone once its lease
// duration has passed. client-go has already judged expiry by its own observation
// before it calls Update, so this check can only delay a takeover, never cause one.
func (l *sqlLock) heldByOther(record *resourcelock.LeaderElectionRecord, now time.Time) bool {
	if record.HolderIdentity == "" || record.HolderIdentity == l.identity {
		return false
	}
	expiry := record.RenewTime.Add(time.Duration(record.LeaseDurationSeconds) * time.Second)
	return expiry.After(now)
}

// sleepContext waits for d or until ctx is done.
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// RecordEvent is a no-op; there is no event stream behind a SQL table.
func (*sqlLock) RecordEvent(string) {}

// Identity returns the identity this lock acquires and renews under.
func (l *sqlLock) Identity() string { return l.identity }

// Describe names the lock for log lines.
func (l *sqlLock) Describe() string { return "leader_lock/" + l.name }
