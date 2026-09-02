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
	// bridgeLegacyLease makes a SQL lock defer to the Lease of the same name and
	// namespace, reached through restCfg, until the SQL lock's first row exists.
	bridgeLegacyLease bool
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

// WithLegacyLeaseLock makes a SQL election defer to the Lease that earlier versions
// of the program held for the same name, in namespace, reached through cfg, until
// the first row of the SQL lock exists. Use it for the release that moves an
// election from the Lease lock to the SQL lock, so that a rolling update in which
// old and new replicas overlap still has one leader. See locks.WithLegacyLock for
// the rules. Drop the call once every replica runs the SQL lock.
func (ec *ElectionConfig) WithLegacyLeaseLock(namespace string, cfg *rest.Config) *ElectionConfig {
	ec.Namespace = namespace
	ec.restCfg = cfg
	ec.bridgeLegacyLease = true
	return ec
}

// resourceLock builds the lock the elector will run against, chosen by
// ResourceLockType. id is the identity the elector will hold the lock under.
func (ec *ElectionConfig) resourceLock(ctx context.Context, id string) (resourcelock.Interface, error) {
	switch ec.ResourceLockType {
	case FileLockType:
		return locks.NewFile(id, ec.Name), nil
	case SQLLockType:
		var opts []locks.SQLOption
		if ec.bridgeLegacyLease {
			legacy, err := ec.leaseLock(id)
			if err != nil {
				return nil, fmt.Errorf("building the legacy lease lock: %w", err)
			}
			opts = append(opts, locks.WithLegacyLock(legacy))
		}
		return locks.NewSQL(ctx, ec.sqlDB, ec.Name, id, opts...)
	default:
		return ec.leaseLock(id)
	}
}

// leaseLock builds the client-go lock for a Lease (or another Kubernetes object,
// per ResourceLockType) of this election's name and namespace.
func (ec *ElectionConfig) leaseLock(id string) (resourcelock.Interface, error) {
	lockType := ec.ResourceLockType
	if lockType == SQLLockType {
		lockType = resourcelock.LeasesResourceLock
	}
	return resourcelock.NewFromKubeconfig(
		lockType,
		ec.Namespace,
		ec.Name,
		resourcelock.ResourceLockConfig{
			Identity: id,
		},
		jsonClientConfig(ec.restCfg),
		ec.TTL/2,
	)
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
