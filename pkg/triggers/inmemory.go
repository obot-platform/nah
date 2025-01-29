package triggers

import (
	"context"
	"sync"

	"github.com/obot-platform/nah/pkg/backend"
	"github.com/obot-platform/nah/pkg/log"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type inMemoryStore struct {
	lock     sync.RWMutex
	matchers map[schema.GroupVersionKind]map[enqueueTarget]map[string]objectMatcher
}

func newInMemoryStore() *inMemoryStore {
	return &inMemoryStore{
		matchers: map[schema.GroupVersionKind]map[enqueueTarget]map[string]objectMatcher{},
	}
}

func (m *inMemoryStore) Close() error {
	return nil
}

func (m *inMemoryStore) Trigger(_ context.Context, trigger backend.Trigger, gvk schema.GroupVersionKind, key, namespace, name string, obj kclient.Object, _ bool) error {
	m.lock.RLock()
	defer m.lock.RUnlock()

	for et, matchers := range m.matchers[gvk] {
		for _, matcher := range matchers {
			if matcher.match(namespace, name, obj) {
				log.Debugf("Triggering [%s] [%v] from [%s] [%v]", et.key, et.gvk, key, gvk)
				if err := trigger.Trigger(et.gvk, et.key, 0); err != nil {
					log.Errorf("failed to trigger %s: %v", et.key, err)
				}
				break
			}
		}
	}

	return nil
}

func (m *inMemoryStore) RegisterMatcher(_ context.Context, gvk schema.GroupVersionKind, key string, sourceGVK schema.GroupVersionKind, _ runtime.Object, mr objectMatcher) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	target := enqueueTarget{
		key: key,
		gvk: gvk,
	}
	matchers, ok := m.matchers[sourceGVK]
	if !ok {
		matchers = map[enqueueTarget]map[string]objectMatcher{}
		m.matchers[sourceGVK] = matchers
	}

	matcherKey := mr.string()
	if _, ok := matchers[target][matcherKey]; ok {
		return nil
	}

	if matchers[target] == nil {
		matchers[target] = map[string]objectMatcher{}
	}

	matchers[target][matcherKey] = mr

	return nil
}

func (m *inMemoryStore) UnregisterMatcher(_ context.Context, trigger backend.Trigger, gvk schema.GroupVersionKind, key, namespace, name string, obj kclient.Object) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	for sourceGVK, matchers := range m.matchers {
		for target, mts := range matchers {
			if target.gvk == gvk && target.key == key {
				// If the target is the GVK and key we are unregistering, then skip it
				delete(matchers, target)
				if len(matchers) == 0 {
					delete(m.matchers, sourceGVK)
				}
				continue
			}
			for _, mt := range mts {
				// If the matcher matches the deleted object exactly, then skip the matcher.
				if sourceGVK == gvk && mt.Namespace == namespace && mt.Name == name {
					delete(matchers[target], mt.string())
					if len(matchers[target]) == 0 {
						delete(matchers, target)
					}
				}
				if sourceGVK == gvk && mt.match(namespace, name, obj) {
					log.Debugf("Triggering [%s] [%v] from [%s] [%v] on delete", target.key, target.gvk, key, gvk)
					_ = trigger.Trigger(target.gvk, target.key, 0)
				}
			}
		}
	}

	return nil
}

func (*inMemoryStore) ShouldHandle(context.Context, schema.GroupVersionKind, kclient.Object) (bool, error) {
	return true, nil
}

type enqueueTarget struct {
	key string
	gvk schema.GroupVersionKind
}
