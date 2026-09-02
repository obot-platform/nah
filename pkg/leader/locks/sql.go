package locks

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

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
)

// sqlGroupResource is what NotFound, AlreadyExists and Conflict errors report. There
// is no API group behind this table; client-go only inspects an error's reason.
var sqlGroupResource = schema.GroupResource{Group: "nah.obot.ai", Resource: "leaderlocks"}

type sqlLock struct {
	db       *sql.DB
	name     string
	identity string

	// version is the row version last observed by Get or written by Create or
	// Update. Zero means neither has happened and Update must refuse to run.
	version int64
}

// NewSQL returns a lock for the election called name, held under identity, backed by
// a row in the leader_lock table of db. It creates the table if it is missing.
//
// Like client-go's LeaseLock and the file lock in this package it is stateful: Get and
// Create remember the row version they observed, and Update succeeds only if that
// version is still current. That check is what stops two replicas from both
// believing they hold the lock. Use one lock per elector, per process.
func NewSQL(ctx context.Context, db *sql.DB, name, identity string) (resourcelock.Interface, error) {
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

	return &sqlLock{db: db, name: name, identity: identity}, nil
}

// Get returns the current record. A missing row is reported as a Kubernetes NotFound
// because that is the only shape client-go's elector reads as "no lock yet, try
// Create".
func (l *sqlLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	var (
		raw     []byte
		version int64
	)
	err := l.db.QueryRowContext(ctx, sqlGet, l.name).Scan(&raw, &version)
	if errors.Is(err, sql.ErrNoRows) {
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
	return &record, raw, nil
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
	return nil
}

// Update replaces the record, but only while the row is still at the version this
// lock last observed. If another replica has written since, no row matches and this
// returns Conflict, which client-go answers by re-reading on its slow path.
func (l *sqlLock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	if l.version == 0 {
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

// RecordEvent is a no-op; there is no event stream behind a SQL table.
func (*sqlLock) RecordEvent(string) {}

// Identity returns the identity this lock acquires and renews under.
func (l *sqlLock) Identity() string { return l.identity }

// Describe names the lock for log lines.
func (l *sqlLock) Describe() string { return "leader_lock/" + l.name }
