package runtime

import (
	"time"

	"github.com/obot-platform/nah/pkg/mapper"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
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
	Rest              *rest.Config
	Namespace         string
	FieldSelector     fields.Selector
	LabelSelector     labels.Selector
	ByObject          map[client.Object]cache.ByObject
	GVKThreadiness    map[schema.GroupVersionKind]int
	GVKQueueSplitters map[schema.GroupVersionKind]WorkerQueueSplitter
}

func NewRuntime(cfg *rest.Config, scheme *runtime.Scheme) (*Runtime, error) {
	return NewRuntimeWithConfig(Config{
		Rest: cfg,
	}, scheme)
}

func NewRuntimeForNamespace(cfg *rest.Config, namespace string, scheme *runtime.Scheme) (*Runtime, error) {
	return NewRuntimeWithConfig(Config{Rest: cfg, Namespace: namespace}, scheme)
}

func NewRuntimeWithConfig(cfg Config, scheme *runtime.Scheme) (*Runtime, error) {
	uncachedClient, cachedClient, theCache, err := getClients(cfg, scheme)
	if err != nil {
		return nil, err
	}

	factory := NewSharedControllerFactory(uncachedClient, theCache, &SharedControllerFactoryOptions{
		KindWorkers:       cfg.GVKThreadiness,
		KindQueueSplitter: cfg.GVKQueueSplitters,
		// In nah this is only invoked when a key fails to process
		DefaultRateLimiter: workqueue.NewTypedMaxOfRateLimiter(
			// This will go .5, 1, 2, 4, 8 seconds, etc up until 15 minutes
			workqueue.NewTypedItemExponentialFailureRateLimiter[any](500*time.Millisecond, 15*time.Minute),
		),
	})

	return &Runtime{
		Backend: newBackend(factory, newCacheClient(uncachedClient, cachedClient), theCache),
	}, nil
}

func getClients(cfg Config, scheme *runtime.Scheme) (uncachedClient client.WithWatch, cachedClient client.Client, theCache cache.Cache, err error) {
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
		Mapper:               mapper,
		Scheme:               scheme,
		DefaultNamespaces:    namespaces,
		DefaultFieldSelector: cfg.FieldSelector,
		DefaultLabelSelector: cfg.LabelSelector,
		ByObject:             cfg.ByObject,
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
