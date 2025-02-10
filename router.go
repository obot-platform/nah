package nah

import (
	"fmt"

	"github.com/obot-platform/nah/pkg/backend"
	"github.com/obot-platform/nah/pkg/leader"
	"github.com/obot-platform/nah/pkg/restconfig"
	"github.com/obot-platform/nah/pkg/router"
	nruntime "github.com/obot-platform/nah/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

const defaultHealthzPort = 8888

type Options struct {
	// If the backend is nil, then DefaultRESTConfig, DefaultNamespace, and Scheme are used to create a backend.
	Backend backend.Backend
	// If a Backend is provided, then this is ignored. If not provided and needed, then a default is created with Scheme.
	DefaultRESTConfig *rest.Config
	// If a Backend is provided, then this is ignored.
	DefaultNamespace string
	// If a Backend is provided, then this is ignored.
	Scheme *runtime.Scheme
	// APIGroupConfigs are keyed by an API group. This indicates to the router that all actions on this group should use the
	// given Config. This is useful for routers that watch different objects on different API servers.
	APIGroupConfigs map[string]nruntime.GroupConfig
	// ElectionConfig being nil represents no leader election for the router.
	ElectionConfig *leader.ElectionConfig
	// Defaults to 8888
	HealthzPort int
	// Change the threadedness per GVK
	GVKThreadiness map[schema.GroupVersionKind]int
	// Split the worker queues for these GVKs
	GVKQueueSplitters map[schema.GroupVersionKind]nruntime.WorkerQueueSplitter
}

func (o *Options) complete() (*Options, error) {
	var result Options
	if o != nil {
		result = *o
	}

	if result.Scheme == nil {
		return nil, fmt.Errorf("scheme is required to be set")
	}

	if result.HealthzPort == 0 {
		result.HealthzPort = defaultHealthzPort
	}

	if result.Backend != nil {
		return &result, nil
	}

	if result.DefaultRESTConfig == nil {
		var err error
		result.DefaultRESTConfig, err = restconfig.New(result.Scheme)
		if err != nil {
			return nil, err
		}
	}

	defaultConfig := nruntime.Config{
		GroupConfig: nruntime.GroupConfig{
			Rest:      result.DefaultRESTConfig,
			Namespace: result.DefaultNamespace,
		},
		GVKThreadiness:    result.GVKThreadiness,
		GVKQueueSplitters: result.GVKQueueSplitters,
	}
	backend, err := nruntime.NewRuntimeWithConfigs(defaultConfig, result.APIGroupConfigs, result.Scheme)
	if err != nil {
		return nil, err
	}
	result.Backend = backend.Backend

	return &result, nil
}

// DefaultOptions represent the standard options for a Router.
// The default leader election uses a lease lock and a TTL of 15 seconds.
func DefaultOptions(routerName string, scheme *runtime.Scheme) (*Options, error) {
	cfg, err := restconfig.New(scheme)
	if err != nil {
		return nil, err
	}
	rt, err := nruntime.NewRuntimeForNamespace(cfg, "", scheme)
	if err != nil {
		return nil, err
	}

	return &Options{
		Backend:           rt.Backend,
		DefaultRESTConfig: cfg,
		Scheme:            scheme,
		ElectionConfig:    leader.NewDefaultElectionConfig("", routerName, cfg),
		HealthzPort:       defaultHealthzPort,
	}, nil
}

// DefaultRouter The routerName is important as this name will be used to assign ownership of objects created by this
// router. Specifically, the routerName is assigned to the sub-context in the apply actions. Additionally, the routerName
// will be used for the leader election lease lock.
func DefaultRouter(routerName string, scheme *runtime.Scheme) (*router.Router, error) {
	opts, err := DefaultOptions(routerName, scheme)
	if err != nil {
		return nil, err
	}
	return NewRouter(routerName, opts)
}

func NewRouter(handlerName string, opts *Options) (*router.Router, error) {
	opts, err := opts.complete()
	if err != nil {
		return nil, err
	}
	return router.New(router.NewHandlerSet(handlerName, opts.Backend.Scheme(), opts.Backend), opts.ElectionConfig, opts.HealthzPort), nil
}
