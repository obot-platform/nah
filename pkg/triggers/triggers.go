package triggers

import (
	"context"
	"database/sql"
	"strings"

	"github.com/obot-platform/nah/pkg/backend"
	"github.com/obot-platform/nah/pkg/log"
	"github.com/obot-platform/nah/pkg/untriggered"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type Triggers struct {
	trigger   backend.Trigger
	gvkLookup backend.Backend
	scheme    *runtime.Scheme
	watcher   Watcher
	store     store
}

type Generationed interface {
	TriggerGeneration() int64
}

type Options struct {
	DB *sql.DB
}

func complete(opts ...Options) Options {
	var result Options
	for _, opt := range opts {
		if opt.DB != nil {
			result.DB = opt.DB
		}
	}
	return result
}

func New(handlerName string, scheme *runtime.Scheme, trigger backend.Trigger, gvkLookup backend.Backend, watcher Watcher, opts ...Options) (*Triggers, error) {
	opt := complete(opts...)
	var (
		s   store
		err error
	)
	if opt.DB != nil {
		s, err = newDBStore(handlerName, opt.DB)
	} else {
		s = newInMemoryStore()
	}
	if err != nil {
		return nil, err
	}

	return newWithStore(scheme, trigger, gvkLookup, watcher, s), nil
}

func newWithStore(scheme *runtime.Scheme, trigger backend.Trigger, gvkLookup backend.Backend, watcher Watcher, store store) *Triggers {
	return &Triggers{
		trigger:   trigger,
		gvkLookup: gvkLookup,
		scheme:    scheme,
		watcher:   watcher,
		store:     store,
	}
}

func (t *Triggers) Close() error {
	return t.store.Close()
}

func (t *Triggers) Register(ctx context.Context, gvk schema.GroupVersionKind, key string, obj, sourceObj runtime.Object, namespace, name string, selector labels.Selector, fields fields.Selector) (schema.GroupVersionKind, bool, error) {
	if untriggered.IsWrapped(sourceObj) {
		return schema.GroupVersionKind{}, false, nil
	}
	sourceGVK, err := t.gvkLookup.GVKForObject(sourceObj, t.scheme)
	if err != nil {
		return sourceGVK, false, err
	}

	if _, ok := sourceObj.(kclient.ObjectList); ok {
		sourceGVK.Kind = strings.TrimSuffix(sourceGVK.Kind, "List")
	}

	if key == (types.NamespacedName{Namespace: namespace, Name: name}.String()) {
		return sourceGVK, false, nil
	}

	if err = t.store.RegisterMatcher(ctx, gvk, key, sourceGVK, obj, objectMatcher{
		Namespace: namespace,
		Name:      name,
		Selector:  selector,
		Fields:    fields,
	}); err != nil {
		return sourceGVK, false, err
	}

	return sourceGVK, true, t.watcher.WatchGVK(sourceGVK)
}

func (t *Triggers) Trigger(ctx context.Context, gvk schema.GroupVersionKind, key, namespace, name string, obj kclient.Object, trackRevision bool) {
	if err := t.store.Trigger(ctx, t.trigger, gvk, key, namespace, name, obj, trackRevision); err != nil {
		log.Errorf("Failed to get matchers for %s: %v", gvk, err)
		return
	}
}

// UnregisterAndTrigger will unregister all triggers for the object, both as source and target.
// If a trigger source matches the object exactly, then the trigger will be invoked.
func (t *Triggers) UnregisterAndTrigger(ctx context.Context, gvk schema.GroupVersionKind, key, namespace, name string, obj kclient.Object) {
	if err := t.store.UnregisterMatcher(ctx, t.trigger, gvk, key, namespace, name, obj); err != nil {
		log.Errorf("Failed to unregister matchers for %s: %v", gvk, err)
		return
	}
}

func (t *Triggers) ShouldHandle(ctx context.Context, gvk schema.GroupVersionKind, obj kclient.Object) (bool, error) {
	return t.store.ShouldHandle(ctx, gvk, obj)
}

type Watcher interface {
	WatchGVK(gvks ...schema.GroupVersionKind) error
}

type store interface {
	RegisterMatcher(context.Context, schema.GroupVersionKind, string, schema.GroupVersionKind, runtime.Object, objectMatcher) error
	Trigger(context.Context, backend.Trigger, schema.GroupVersionKind, string, string, string, kclient.Object, bool) error
	UnregisterMatcher(context.Context, backend.Trigger, schema.GroupVersionKind, string, string, string, kclient.Object) error
	ShouldHandle(context.Context, schema.GroupVersionKind, kclient.Object) (bool, error)
	Close() error
}
