package triggers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/obot-platform/nah/pkg/backend"
	"github.com/obot-platform/nah/pkg/log"
	"github.com/obot-platform/nah/pkg/triggers/statements"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func newDBStore(handlerName string, db *sql.DB) (store, error) {
	statements := statements.New(handlerName)
	_, err := db.Exec(statements.CreateRevisionsTable())
	if err != nil {
		return nil, fmt.Errorf("error creating table: %v", err)
	}

	return &dbStore{
		db:              db,
		statements:      statements,
		triggerCache:    make(map[string]int64),
		generationCache: make(map[string]int64),
	}, nil
}

type dbStore struct {
	lock            sync.RWMutex
	db              *sql.DB
	statements      *statements.Statements
	triggerCache    map[string]int64
	generationCache map[string]int64
}

func (d *dbStore) Close() error {
	return d.db.Close()
}

func (d *dbStore) Trigger(ctx context.Context, trigger backend.Trigger, gvk schema.GroupVersionKind, key, namespace, name string, obj kclient.Object, trackRevision bool) (err error) {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}()

	if shouldTrigger, err := d.shouldTrigger(ctx, tx, gvk, obj, trackRevision); err != nil || !shouldTrigger {
		return err
	}

	if err := d.createMatchersTable(ctx, tx, gvk.Kind); err != nil {
		return fmt.Errorf("failed to create matchers table for %s: %w", gvk.Kind, err)
	}

	return d.trigger(ctx, tx, trigger, gvk, key, namespace, name, obj)
}

func (d *dbStore) RegisterMatcher(ctx context.Context, gvk schema.GroupVersionKind, key string, sourceGVK schema.GroupVersionKind, sourceObj runtime.Object, matcher objectMatcher) error {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}()

	if err := d.createMatchersTable(ctx, tx, sourceGVK.Kind); err != nil {
		return fmt.Errorf("failed to create matchers table for %s: %w", sourceGVK.Kind, err)
	}

	var labelSelector, fieldSelector string
	if matcher.Selector != nil {
		labelSelector = matcher.Selector.String()
	}
	if matcher.Fields != nil {
		fieldSelector = matcher.Fields.String()
	}

	apiVersion, kind := gvk.ToAPIVersionAndKind()

	d.lock.RLock()
	generation, ok := d.generationCache[gvk.Kind]
	d.lock.RUnlock()
	if !ok {
		if o, ok := sourceObj.(Generationed); ok {
			generation = o.TriggerGeneration()
		}
		d.lock.Lock()
		d.generationCache[sourceGVK.Kind] = generation
		d.lock.Unlock()
	}

	// Best effort
	_, err = tx.ExecContext(ctx, d.statements.ClearOldMatchers(sourceGVK.Kind), generation, apiVersion, kind, key)
	if err != nil {
		log.Warnf("failed to clear old matchers for %s/%s %s: %v", apiVersion, kind, key, err)
	}

	_, err = tx.ExecContext(ctx, d.statements.InsertMatcher(sourceGVK.Kind), generation, apiVersion, kind, key, matcher.Namespace, matcher.Name, labelSelector, fieldSelector)
	return err
}

func (d *dbStore) UnregisterMatcher(ctx context.Context, trigger backend.Trigger, gvk schema.GroupVersionKind, key, namespace, name string, obj kclient.Object) error {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}()

	var errs []error

	if shouldTrigger, err := d.shouldTrigger(ctx, tx, gvk, obj, true); err == nil && shouldTrigger {
		// Even if there is an error during triggering, try to clean everything up.
		// We won't trigger on this again after it is removed.
		if triggerErr := d.trigger(ctx, tx, trigger, gvk, key, namespace, name, obj); triggerErr != nil {
			errs = append(errs, triggerErr)
		}
	} else if err != nil {
		errs = append(errs, err)
	}

	// The cleanup is really all best-effort. The object is getting deleted, so we won't trigger on it again unless it is recreated.
	apiVersion, kind := gvk.ToAPIVersionAndKind()
	// Delete all triggers from this object.
	for _, statement := range d.statements.DeleteMatchers() {
		if _, err = tx.ExecContext(ctx, statement, apiVersion, kind, key); err != nil {
			errs = append(errs, fmt.Errorf("failed to delete matchers for %s: %w", gvk.Kind, err))
		}
	}

	if _, err = tx.ExecContext(ctx, d.statements.DeleteExactMatchers(gvk.Kind), namespace, name); err != nil {
		errs = append(errs, fmt.Errorf("failed to delete exact matchers for %s: %w", gvk.Kind, err))
	}

	return errors.Join(errs...)
}

func (d *dbStore) trigger(ctx context.Context, tx *sql.Tx, trigger backend.Trigger, gvk schema.GroupVersionKind, key, namespace, name string, obj kclient.Object) error {
	d.lock.RLock()
	generation, ok := d.generationCache[gvk.Kind]
	d.lock.RUnlock()
	if !ok {
		if o, ok := obj.(Generationed); ok {
			generation = o.TriggerGeneration()
		}
		d.lock.Lock()
		d.generationCache[gvk.Kind] = generation
		d.lock.Unlock()
	}

	rows, err := tx.QueryContext(ctx, d.statements.GetMatchers(gvk.Kind), generation, namespace, name)
	if err != nil {
		return fmt.Errorf("failed to get matchers for %s: %w", gvk.Kind, err)
	}
	defer rows.Close()

	var (
		targetAPIVersion, targetKind, targetKey                           string
		matcherNamespace, matcherName, labelSelectorStr, fieldSelectorStr string
		matcher                                                           objectMatcher
	)
	for rows.Next() {
		if err = rows.Scan(&targetAPIVersion, &targetKind, &targetKey, &matcherNamespace, &matcherName, &labelSelectorStr, &fieldSelectorStr); err != nil {
			return fmt.Errorf("failed to scan matchers for %s: %w", gvk.Kind, err)
		}

		if labelSelectorStr != "" {
			matcher.Selector, err = labels.Parse(labelSelectorStr)
			if err != nil {
				return fmt.Errorf("failed to parse label selector %q for %s: %w", labelSelectorStr, gvk.Kind, err)
			}
		} else {
			matcher.Selector = labels.Everything()
		}

		if fieldSelectorStr != "" {
			matcher.Fields, err = fields.ParseSelector(fieldSelectorStr)
			if err != nil {
				return fmt.Errorf("failed to parse field selector %q for %s: %w", fieldSelectorStr, gvk.Kind, err)
			}
		} else {
			matcher.Fields = fields.Everything()
		}

		matcher.Namespace = matcherNamespace
		matcher.Name = matcherName

		targetGVK := schema.FromAPIVersionAndKind(targetAPIVersion, targetKind)

		if matcher.match(namespace, name, obj) {
			log.Debugf("Triggering [%s] [%v] from [%s] [%v]", targetKey, targetGVK, key, gvk)
			if err = trigger.Trigger(targetGVK, targetKey, 0); err != nil {
				log.Errorf("failed to trigger %s: %v", targetKey, err)
			}
		}
	}

	if err = rows.Err(); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	return nil
}

func (d *dbStore) shouldTrigger(ctx context.Context, tx *sql.Tx, gvk schema.GroupVersionKind, obj kclient.Object, trackRevision bool) (bool, error) {
	if obj == nil {
		return true, nil
	}
	updated, err := d.shouldHandleTx(ctx, tx, gvk, obj, trackRevision)
	if err != nil {
		return false, fmt.Errorf("failed to store revision: %w", err)
	}
	return updated, nil
}

func (d *dbStore) createMatchersTable(ctx context.Context, tx *sql.Tx, kind string) error {
	s := d.statements.CreateMatchersTable(kind)
	if s == "" {
		// Trigger table already created
		return nil
	}

	_, err := tx.ExecContext(ctx, s)
	return err
}

func (d *dbStore) shouldHandleTx(ctx context.Context, tx *sql.Tx, gvk schema.GroupVersionKind, obj kclient.Object, updateStore bool) (bool, error) {
	if obj == nil {
		return true, nil
	}

	rev, err := strconv.ParseInt(obj.GetResourceVersion(), 10, 64)
	if err != nil {
		return false, nil
	}

	kind := gvk.Kind

	d.lock.RLock()
	latest := d.triggerCache[kind]
	d.lock.RUnlock()
	if rev <= latest {
		return false, nil
	}

	d.lock.Lock()
	defer d.lock.Unlock()
	if latest = d.triggerCache[kind]; rev <= latest {
		return false, nil
	}

	generation, ok := d.generationCache[kind]
	if !ok {
		if o, ok := obj.(Generationed); ok {
			generation = o.TriggerGeneration()
		}

		d.generationCache[kind] = generation
	}

	if err := tx.QueryRowContext(ctx, d.statements.GetLatestRevision(), kind, generation).Scan(&latest); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("error getting latest revision: %v", err)
	}

	if rev <= latest {
		d.triggerCache[kind] = latest
		return false, nil
	}

	if updateStore {
		if _, err := tx.ExecContext(ctx, d.statements.UpdateLatestRevision(), kind, rev, generation); err != nil {
			return false, fmt.Errorf("failed to update latest revision for %q: %w", kind, err)
		}

		d.triggerCache[kind] = rev
	}

	return true, nil
}

func (d *dbStore) ShouldHandle(ctx context.Context, gvk schema.GroupVersionKind, obj kclient.Object) (_ bool, err error) {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		// Best effort, default to handling the object.
		log.Warnf("shouldHandle: failed to begin transaction: %v", err)
		return true, nil
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}()

	return d.shouldHandleTx(ctx, tx, gvk, obj, false)
}
