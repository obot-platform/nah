package leader

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/obot-platform/nah/pkg/leader/locks"
	"github.com/obot-platform/nah/pkg/log"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	defaultLeaderTTL = time.Minute
	devLeaderTTL     = time.Hour

	FileLockType = "file"
	SQLLockType  = "sql"
)

type OnLeader func(context.Context) error
type OnNewLeader func(string)

type ElectionConfig struct {
	TTL                               time.Duration
	Name, Namespace, ResourceLockType string
	restCfg                           *rest.Config
	sqlDB                             *sql.DB
}

func NewDefaultElectionConfig(namespace, name string, cfg *rest.Config) *ElectionConfig {
	return &ElectionConfig{
		TTL:              defaultElectionTTL(),
		Namespace:        namespace,
		Name:             name,
		ResourceLockType: resourcelock.LeasesResourceLock,
		restCfg:          cfg,
	}
}

func NewFileElectionConfig(fileName string) *ElectionConfig {
	return &ElectionConfig{
		TTL:              defaultElectionTTL(),
		Name:             fileName,
		ResourceLockType: FileLockType,
	}
}

func NewFileElectionConfigWithTTL(fileName string, ttl time.Duration) *ElectionConfig {
	return &ElectionConfig{
		TTL:              ttl,
		Name:             fileName,
		ResourceLockType: FileLockType,
	}
}

func NewElectionConfig(ttl time.Duration, namespace, name, lockType string, cfg *rest.Config) *ElectionConfig {
	return &ElectionConfig{
		TTL:              ttl,
		Namespace:        namespace,
		Name:             name,
		ResourceLockType: lockType,
		restCfg:          cfg,
	}
}

// NewSQLElectionConfig returns a config whose lock is a row in a leader_lock table in
// db rather than a Lease or a file. TTL, RenewDeadline and RetryPeriod are the same as
// the default config; only the storage behind the lock differs. Use this when the
// controller's own store is SQL-backed, where a Lease in a versioned object store is
// the most expensive object in the database.
func NewSQLElectionConfig(name string, db *sql.DB) *ElectionConfig {
	return &ElectionConfig{
		TTL:              defaultElectionTTL(),
		Name:             name,
		ResourceLockType: SQLLockType,
		sqlDB:            db,
	}
}

// resourceLock builds the lock the elector will run against, chosen by
// ResourceLockType. id is the identity the elector will hold the lock under.
func (ec *ElectionConfig) resourceLock(ctx context.Context, id string) (resourcelock.Interface, error) {
	switch ec.ResourceLockType {
	case FileLockType:
		return locks.NewFile(id, ec.Name), nil
	case SQLLockType:
		return locks.NewSQL(ctx, ec.sqlDB, ec.Name, id)
	default:
		return resourcelock.NewFromKubeconfig(
			ec.ResourceLockType,
			ec.Namespace,
			ec.Name,
			resourcelock.ResourceLockConfig{
				Identity: id,
			},
			jsonClientConfig(ec.restCfg),
			ec.TTL/2,
		)
	}
}

func defaultElectionTTL() time.Duration {
	if os.Getenv("NAH_DEV_MODE") != "" {
		return devLeaderTTL
	}
	return defaultLeaderTTL
}

func (ec *ElectionConfig) Run(ctx context.Context, id string, onLeader OnLeader, onSwitchLeader OnNewLeader, signalDone func()) error {
	if ec == nil {
		// Don't start leader election if there is no config.
		return onLeader(ctx)
	}

	if ec.Namespace == "" {
		ec.Namespace = "kube-system"
	}

	if err := ec.run(ctx, id, onLeader, onSwitchLeader, signalDone); err != nil {
		return fmt.Errorf("failed to start leader election for %s: %v", ec.Name, err)
	}

	return nil
}

func (ec *ElectionConfig) run(ctx context.Context, id string, cb OnLeader, onSwitchLeader OnNewLeader, signalDone func()) error {
	rl, err := ec.resourceLock(ctx, id)
	if err != nil {
		return fmt.Errorf("error creating leader lock for %s: %v", ec.Name, err)
	}

	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: ec.TTL,
		RenewDeadline: ec.TTL / 2,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				if err := cb(ctx); err != nil {
					log.Fatalf("leader callback error: %v", err)
				}
			},
			OnNewLeader: onSwitchLeader,
			OnStoppedLeading: func() {
				select {
				case <-ctx.Done():
					log.Infof("requested to terminate, exiting")
					if signalDone != nil {
						signalDone()
					}
				default:
					log.Fatalf("leader election lost for %s", ec.Name)
				}
			},
		},
		ReleaseOnCancel: true,
	})
	if err != nil {
		return err
	}

	go func() {
		le.Run(ctx)
	}()
	return nil
}

func jsonClientConfig(cfg *rest.Config) *rest.Config {
	cfg = rest.CopyConfig(cfg)
	cfg.AcceptContentTypes = runtime.ContentTypeJSON
	cfg.ContentType = runtime.ContentTypeJSON
	return cfg
}
