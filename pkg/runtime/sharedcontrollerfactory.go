package runtime

import (
	"context"
	"maps"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type SharedControllerFactory interface {
	ForKind(ctx context.Context, gvk schema.GroupVersionKind) (SharedController, error)
	Preload(ctx context.Context) error
	Start(ctx context.Context) error
}

type SharedControllerFactoryOptions struct {
	DefaultRateLimiter workqueue.TypedRateLimiter[any]
	DefaultWorkers     int

	KindRateLimiter   map[schema.GroupVersionKind]workqueue.TypedRateLimiter[any]
	KindWorkers       map[schema.GroupVersionKind]int
	KindQueueSplitter map[schema.GroupVersionKind]WorkerQueueSplitter
}

type sharedControllerFactory struct {
	controllerLock sync.RWMutex
	cacheStartLock sync.Mutex

	cache        cache.Cache
	cacheStarted bool
	client       kclient.Client
	controllers  map[schema.GroupVersionKind]*sharedController

	rateLimiter       workqueue.TypedRateLimiter[any]
	workers           int
	kindRateLimiter   map[schema.GroupVersionKind]workqueue.TypedRateLimiter[any]
	kindWorkers       map[schema.GroupVersionKind]int
	kindQueueSplitter map[schema.GroupVersionKind]WorkerQueueSplitter
}

func NewSharedControllerFactory(c kclient.Client, cache cache.Cache, opts *SharedControllerFactoryOptions) SharedControllerFactory {
	opts = applyDefaultSharedOptions(opts)
	return &sharedControllerFactory{
		cache:             cache,
		client:            c,
		controllers:       map[schema.GroupVersionKind]*sharedController{},
		workers:           opts.DefaultWorkers,
		kindWorkers:       opts.KindWorkers,
		rateLimiter:       opts.DefaultRateLimiter,
		kindRateLimiter:   opts.KindRateLimiter,
		kindQueueSplitter: opts.KindQueueSplitter,
	}
}

func applyDefaultSharedOptions(opts *SharedControllerFactoryOptions) *SharedControllerFactoryOptions {
	var newOpts SharedControllerFactoryOptions
	if opts != nil {
		newOpts = *opts
	}
	if newOpts.DefaultWorkers == 0 {
		newOpts.DefaultWorkers = DefaultThreadiness
	}
	return &newOpts
}
func (s *sharedControllerFactory) Preload(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "sharedControllerFactoryPreload")
	defer span.End()

	return s.loadAndStart(ctx, false)
}

func (s *sharedControllerFactory) Start(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "sharedControllerFactoryStart")
	defer span.End()

	return s.loadAndStart(ctx, true)
}

func (s *sharedControllerFactory) loadAndStart(ctx context.Context, start bool) error {
	ctx, span := tracer.Start(ctx, "sharedControllerFactoryLoadAndStart")
	defer span.End()

	s.controllerLock.Lock()
	defer s.controllerLock.Unlock()

	go func() {
		s.cacheStartLock.Lock()
		defer s.cacheStartLock.Unlock()
		if s.cacheStarted {
			return
		}
		if err := s.cache.Start(ctx); err != nil {
			panic(err)
		}
		s.cacheStarted = true
	}()

	// copy so we can release the lock during cache wait
	controllersCopy := make(map[schema.GroupVersionKind]*sharedController, len(s.controllers))
	maps.Copy(controllersCopy, s.controllers)

	// Do not hold lock while waiting because this can cause a deadlock if
	// one of the handlers you are waiting on tries to acquire this lock (by looking up
	// shared controller)
	s.controllerLock.Unlock()
	span.AddEvent("waiting for cache sync")
	s.cache.WaitForCacheSync(ctx)
	s.controllerLock.Lock()

	if start {
		for gvk, controller := range controllersCopy {
			w, err := s.getWorkers(gvk)
			if err != nil {
				return err
			}
			if err := controller.Start(ctx, w); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *sharedControllerFactory) ForKind(ctx context.Context, gvk schema.GroupVersionKind) (SharedController, error) {
	controllerResult := s.byGVK(gvk)
	if controllerResult != nil {
		return controllerResult, nil
	}

	s.controllerLock.Lock()
	defer s.controllerLock.Unlock()

	controllerResult = s.controllers[gvk]
	if controllerResult != nil {
		return controllerResult, nil
	}

	handler := &SharedHandler{gvk: gvk}

	controllerResult = &sharedController{
		deferredController: func() (Controller, error) {
			rateLimiter, ok := s.kindRateLimiter[gvk]
			if !ok {
				rateLimiter = s.rateLimiter
			}

			return New(ctx, gvk, s.client.Scheme(), s.cache, handler, &Options{
				RateLimiter:   rateLimiter,
				QueueSplitter: s.kindQueueSplitter[gvk],
			})
		},
		handler: handler,
		client:  s.client,
		gvk:     gvk,
	}

	s.controllers[gvk] = controllerResult
	return controllerResult, nil
}

func (s *sharedControllerFactory) getWorkers(gvk schema.GroupVersionKind) (int, error) {
	if w, ok := s.kindWorkers[gvk]; ok {
		return w, nil
	}
	return s.workers, nil
}

func (s *sharedControllerFactory) byGVK(gvk schema.GroupVersionKind) *sharedController {
	s.controllerLock.RLock()
	defer s.controllerLock.RUnlock()
	return s.controllers[gvk]
}
