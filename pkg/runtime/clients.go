package runtime

import (
	"time"

	"github.com/obot-platform/nah/pkg/mapper"
	"github.com/obot-platform/nah/pkg/runtime/multi"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Runtime struct {
	Backend *Backend
}

type Config struct {
	GroupConfig
	GVKThreadiness    map[schema.GroupVersionKind]int
	GVKQueueSplitters map[schema.GroupVersionKind]WorkerQueueSplitter
}

type GroupConfig struct {
	Rest      *rest.Config
	Namespace string
}

func NewRuntime(cfg *rest.Config, scheme *runtime.Scheme) (*Runtime, error) {
	return NewRuntimeWithConfig(Config{
		GroupConfig: GroupConfig{
			Rest: cfg,
		},
	}, scheme)
}

func NewRuntimeForNamespace(cfg *rest.Config, namespace string, scheme *runtime.Scheme) (*Runtime, error) {
	return NewRuntimeWithConfigs(Config{GroupConfig: GroupConfig{Rest: cfg, Namespace: namespace}}, nil, scheme)
}

func NewRuntimeWithConfig(cfg Config, scheme *runtime.Scheme) (*Runtime, error) {
	return NewRuntimeWithConfigs(cfg, nil, scheme)
}

func NewRuntimeWithConfigs(defaultConfig Config, apiGroupConfigs map[string]GroupConfig, scheme *runtime.Scheme) (*Runtime, error) {
	clients := make(map[string]client.WithWatch, len(apiGroupConfigs))
	cachedClients := make(map[string]client.Client, len(apiGroupConfigs))
	caches := make(map[string]cache.Cache, len(apiGroupConfigs))

	for key, cfg := range apiGroupConfigs {
		uncachedClient, cachedClient, theCache, err := getClients(cfg, scheme)
		if err != nil {
			return nil, err
		}

		clients[key] = uncachedClient
		caches[key] = theCache
		cachedClients[key] = cachedClient
	}

	uncachedClient, cachedClient, theCache, err := getClients(defaultConfig.GroupConfig, scheme)
	if err != nil {
		return nil, err
	}

	aggUncachedClient := multi.NewWithWatch(uncachedClient, clients)
	aggCachedClient := multi.NewClient(cachedClient, cachedClients)
	aggCache := multi.NewCache(scheme, theCache, caches)

	factory := NewSharedControllerFactory(aggUncachedClient, aggCache, &SharedControllerFactoryOptions{
		KindWorkers:       defaultConfig.GVKThreadiness,
		KindQueueSplitter: defaultConfig.GVKQueueSplitters,
		// In nah this is only invoked when a key fails to process
		DefaultRateLimiter: workqueue.NewTypedMaxOfRateLimiter(
			// This will go .5, 1, 2, 4, 8 seconds, etc up until 15 minutes
			workqueue.NewTypedItemExponentialFailureRateLimiter[any](500*time.Millisecond, 15*time.Minute),
		),
	})

	return &Runtime{
		Backend: newBackend(factory, newCacheClient(aggUncachedClient, aggCachedClient), aggCache),
	}, nil
}

func getClients(cfg GroupConfig, scheme *runtime.Scheme) (uncachedClient client.WithWatch, cachedClient client.Client, theCache cache.Cache, err error) {
	mapper, err := mapper.New(cfg.Rest)
	if err != nil {
		return nil, nil, nil, err
	}

	uncachedClient, err = client.NewWithWatch(cfg.Rest, client.Options{
		Scheme: scheme,
		Mapper: mapper,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	var namespaces map[string]cache.Config
	if cfg.Namespace != "" {
		namespaces = map[string]cache.Config{}
		namespaces[cfg.Namespace] = cache.Config{}
	}

	theCache, err = cache.New(cfg.Rest, cache.Options{
		Mapper:            mapper,
		Scheme:            scheme,
		DefaultNamespaces: namespaces,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	cachedClient, err = client.New(cfg.Rest, client.Options{
		Scheme: scheme,
		Mapper: mapper,
		Cache: &client.CacheOptions{
			Reader: theCache,
		},
	})
	if err != nil {
		return nil, nil, nil, err
	}

	return uncachedClient, cachedClient, theCache, nil
}
