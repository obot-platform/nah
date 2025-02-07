package leader

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/obot-platform/nah/pkg/log"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	defaultLeaderTTL = time.Minute
	devLeaderTTL     = time.Hour
)

type OnLeader func(context.Context) error
type OnNewLeader func(string)

type ElectionConfig struct {
	TTL                               time.Duration
	Name, Namespace, ResourceLockType string
	restCfg                           *rest.Config
}

func NewDefaultElectionConfig(namespace, name string, cfg *rest.Config) *ElectionConfig {
	ttl := defaultLeaderTTL
	if os.Getenv("NAH_DEV_MODE") != "" {
		ttl = devLeaderTTL
	}
	return &ElectionConfig{
		TTL:              ttl,
		Namespace:        namespace,
		Name:             name,
		ResourceLockType: resourcelock.LeasesResourceLock,
		restCfg:          cfg,
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
	rl, err := resourcelock.NewFromKubeconfig(
		ec.ResourceLockType,
		ec.Namespace,
		ec.Name,
		resourcelock.ResourceLockConfig{
			Identity: id,
		},
		ec.restCfg,
		ec.TTL/2,
	)
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
					// The context has been canceled or is otherwise complete.
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
