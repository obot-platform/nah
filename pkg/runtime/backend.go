package runtime

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/obot-platform/nah/pkg/backend"
	"github.com/obot-platform/nah/pkg/fields"
	"github.com/obot-platform/nah/pkg/router"
	"github.com/obot-platform/nah/pkg/untriggered"
	"go.opentelemetry.io/otel"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kcache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

var (
	DefaultThreadiness = 10
	tracer             = otel.Tracer("nah/runtime")
)

func init() {
	i, _ := strconv.Atoi(os.Getenv("NAH_THREADINESS"))
	if i > 0 {
		DefaultThreadiness = i
	}
}

type Backend struct {
	*cacheClient

	cacheFactory SharedControllerFactory
	cache        cache.Cache
	startedLock  *sync.RWMutex
	started      bool
}

func newBackend(cacheFactory SharedControllerFactory, client *cacheClient, cache cache.Cache) *Backend {
	return &Backend{
		cacheClient:  client,
		cacheFactory: cacheFactory,
		cache:        cache,
		startedLock:  new(sync.RWMutex),
	}
}

func (b *Backend) Start(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "start")
	defer span.End()

	return b.start(ctx, false)
}

func (b *Backend) Preload(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "preload")
	defer span.End()

	return b.start(ctx, true)
}

func (b *Backend) start(ctx context.Context, preloadOnly bool) (err error) {
	ctx, span := tracer.Start(ctx, "start")
	defer span.End()

	b.startedLock.Lock()
	defer b.startedLock.Unlock()
	defer func() {
		if err == nil {
			b.started = true
		}
	}()
	if preloadOnly {
		err = b.cacheFactory.Preload(ctx)
	} else {
		err = b.cacheFactory.Start(ctx)
	}
	if err != nil {
		return err
	}

	span.AddEvent("waiting for caches to sync")
	if !b.cache.WaitForCacheSync(ctx) {
		return fmt.Errorf("failed to wait for caches to sync")
	}
	if !b.started {
		b.cacheClient.startPurge(ctx)
	}

	return nil
}

func (b *Backend) Trigger(ctx context.Context, gvk schema.GroupVersionKind, key string, delay time.Duration) error {
	controller, err := b.cacheFactory.ForKind(ctx, gvk)
	if err != nil {
		return err
	}

	controller.EnqueueKeyAfter(router.TriggerPrefix+key, delay)
	return nil
}

func (b *Backend) addIndexer(ctx context.Context, gvk schema.GroupVersionKind) error {
	obj, err := b.Scheme().New(gvk)
	if err != nil {
		return err
	}
	f, ok := obj.(fields.Fields)
	if !ok {
		return nil
	}

	cache, err := b.cache.GetInformerForKind(ctx, gvk)
	if err != nil {
		return err
	}

	indexers := map[string]kcache.IndexFunc{}
	for _, field := range f.FieldNames() {
		field := field
		indexers["field:"+field] = func(obj any) ([]string, error) {
			f, ok := obj.(fields.Fields)
			if !ok {
				return nil, nil
			}
			v := f.Get(field)
			if v == "" {
				return nil, nil
			}
			vals := []string{keyFunc("", v)}
			if ko, ok := obj.(kclient.Object); ok && ko.GetNamespace() != "" {
				vals = append(vals, keyFunc(ko.GetNamespace(), v))
			}
			return vals, nil
		}
	}
	return cache.AddIndexers(indexers)
}

func (b *Backend) Watcher(ctx context.Context, gvk schema.GroupVersionKind, name string, cb backend.Callback) error {
	ctx, span := tracer.Start(ctx, "watcher")
	defer span.End()

	c, err := b.cacheFactory.ForKind(ctx, gvk)
	if err != nil {
		return err
	}
	if err := b.addIndexer(ctx, gvk); err != nil {
		return err
	}
	handler := SharedControllerHandlerFunc(func(ctx context.Context, key string, obj runtime.Object) (runtime.Object, error) {
		return cb(ctx, gvk, key, obj)
	})
	if err := c.RegisterHandler(ctx, fmt.Sprintf("%s %v", name, gvk), handler); err != nil {
		return err
	}

	if b.hasStarted() {
		return c.Start(ctx, DefaultThreadiness)
	}
	return nil
}

func (b *Backend) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return b.uncached.GroupVersionKindFor(obj)
}

func (b *Backend) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	return b.uncached.IsObjectNamespaced(obj)
}

func (b *Backend) GVKForObject(obj runtime.Object, scheme *runtime.Scheme) (schema.GroupVersionKind, error) {
	return apiutil.GVKForObject(untriggered.Unwrap(obj), scheme)
}

func (b *Backend) IndexField(ctx context.Context, obj kclient.Object, field string, extractValue kclient.IndexerFunc) error {
	ctx, span := tracer.Start(ctx, "indexField")
	defer span.End()

	return b.cache.IndexField(ctx, obj, field, extractValue)
}

func (b *Backend) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind) (kcache.SharedIndexInformer, error) {
	ctx, span := tracer.Start(ctx, "getInformerForKind")
	defer span.End()

	i, err := b.cache.GetInformerForKind(ctx, gvk)
	if err != nil {
		return nil, err
	}
	return i.(kcache.SharedIndexInformer), nil
}

func (b *Backend) hasStarted() bool {
	b.startedLock.RLock()
	defer b.startedLock.RUnlock()
	return b.started
}
