package router

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/obot-platform/nah/pkg/backend"
	"github.com/obot-platform/nah/pkg/log"
	"github.com/obot-platform/nah/pkg/untriggered"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type triggers struct {
	// matchers has the logical structure as map[groupVersionKind]map[enqueueTarget]map[string]objectMatcher but
	// with sync.Map's
	matchers sync.Map
	// toTrigger is a map of triggerKeys to last seen object. nil value means it this is a remove
	toTrigger      map[triggerKey]kclient.Object
	triggerLock    *sync.Cond
	triggerRunning bool
	trigger        backend.Trigger
	gvkLookup      backend.Backend
	scheme         *runtime.Scheme
	watcher        watcher
}

type watcher interface {
	WatchGVK(gvks ...schema.GroupVersionKind) error
}

type triggerKey struct {
	name      string
	namespace string
	gvk       schema.GroupVersionKind
}

type enqueueTarget struct {
	key string
	gvk schema.GroupVersionKind
}

func (et enqueueTarget) MarshalText() ([]byte, error) {
	return []byte(et.gvk.String() + ": " + et.key), nil
}

func (m *triggers) register(gvk schema.GroupVersionKind, key string, targetGVK schema.GroupVersionKind, mr objectMatcher) {
	matcherKey := mr.String()
	target := enqueueTarget{
		key: key,
		gvk: gvk,
	}

	matchers, _ := m.matchers.LoadOrStore(groupVersionKind{targetGVK}, &sync.Map{})
	matchersByKey, _ := matchers.(*sync.Map).LoadOrStore(target, &sync.Map{})
	_, _ = matchersByKey.(*sync.Map).LoadOrStore(matcherKey, mr)
}

func (m *triggers) Trigger(req Request) {
	m.triggerLock.L.Lock()
	defer m.triggerLock.L.Unlock()
	if m.toTrigger == nil {
		m.toTrigger = map[triggerKey]kclient.Object{}
	}
	m.toTrigger[triggerKey{req.Name, req.Namespace, req.GVK}] = req.Object
	m.kick()
}

func (m *triggers) Register(sourceGVK schema.GroupVersionKind, key string, obj runtime.Object, namespace, name string, selector labels.Selector, fields fields.Selector) (schema.GroupVersionKind, bool, error) {
	if untriggered.IsWrapped(obj) {
		return schema.GroupVersionKind{}, false, nil
	}
	gvk, err := m.gvkLookup.GVKForObject(obj, m.scheme)
	if err != nil {
		return gvk, false, err
	}

	if _, ok := obj.(kclient.ObjectList); ok {
		gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")
	}

	m.register(sourceGVK, key, gvk, objectMatcher{
		Namespace: namespace,
		Name:      name,
		Selector:  selector,
		Fields:    fields,
	})

	return gvk, true, m.watcher.WatchGVK(gvk)
}

func (m *triggers) kick() {
	if m.triggerRunning {
		m.triggerLock.Broadcast()
		return
	}
	m.triggerRunning = true
	go func() {
		m.triggerLock.L.Lock()
		defer m.triggerLock.L.Unlock()
		for {
			if len(m.toTrigger) == 0 {
				m.triggerLock.Wait()
				continue
			}

			workingTrigger := m.toTrigger
			m.toTrigger = nil

			func() {
				// We release the lock here to allow other goroutines to run while we are processing
				m.triggerLock.L.Unlock()
				defer m.triggerLock.L.Lock()

				start := time.Now()
				var checks int
				defer func() {
					duration := time.Since(start)
					log.Debugf("Triggers took %s for %d keys and %d checks", duration, len(workingTrigger), checks)
				}()

				checks = m.process(context.Background(), workingTrigger)
			}()
		}
	}()
}

// UnregisterAndTrigger will unregister all triggers for the object, both as source and target.
// If a trigger source matches the object exactly, then the trigger will be invoked.
func (m *triggers) UnregisterAndTrigger(req Request) {
	m.triggerLock.L.Lock()
	defer m.triggerLock.L.Unlock()

	if m.toTrigger == nil {
		m.toTrigger = map[triggerKey]kclient.Object{}
	}
	m.toTrigger[triggerKey{req.Name, req.Namespace, req.GVK}] = nil
	m.kick()
}

func toKey(namespace string, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}

func (m *triggers) process(ctx context.Context, pending map[triggerKey]kclient.Object) int {
	var checks int
	m.matchers.Range(func(key, value interface{}) bool {
		targetGVK := key.(groupVersionKind)
		matchers := value.(*sync.Map)
		matchers.Range(func(key, value any) bool {
			target := key.(enqueueTarget)

			value.(*sync.Map).Range(func(_, value any) bool {
				checks++
				mt := value.(objectMatcher)
				if mt.Name == "" {
					for key, obj := range pending {
						if key.gvk == targetGVK.GroupVersionKind && mt.Match(key.namespace, key.name, obj) {
							log.Debugf("Triggering [%s] [%v] from [%s] [%v] by selector", target.key, target.gvk, toKey(key.namespace, key.name), targetGVK.GroupVersionKind)
							_ = m.trigger.Trigger(ctx, target.gvk, target.key, 0)
						}
					}
				} else if _, ok := pending[triggerKey{mt.Name, mt.Namespace, targetGVK.GroupVersionKind}]; ok {
					log.Debugf("Triggering [%s] [%v] from [%s] [%v] by direct match", target.key, target.gvk, toKey(mt.Namespace, mt.Name), targetGVK.GroupVersionKind)
					_ = m.trigger.Trigger(ctx, target.gvk, target.key, 0)
				}
				return true
			})

			return true
		})

		// Do deletes after the fact to avoid race conditions
		var deleteKeys []string

		for key, obj := range pending {
			if obj == nil {
				matchers.Delete(enqueueTarget{
					key: toKey(key.namespace, key.name),
					gvk: key.gvk,
				})
				if key.gvk == targetGVK.GroupVersionKind {
					deleteKey := objectMatcher{
						Namespace: key.namespace,
						Name:      key.name,
					}
					deleteKeys = append(deleteKeys, deleteKey.String())
				}
			}
		}

		if len(deleteKeys) > 0 {
			matchers.Range(func(key, value any) bool {
				for _, deleteKey := range deleteKeys {
					value.(*sync.Map).Delete(deleteKey)
				}

				return true
			})
		}

		return true
	})

	return checks
}

func (m *triggers) Dump(indent bool) ([]byte, error) {
	matchers := map[groupVersionKind]map[enqueueTarget]map[string]objectMatcher{}

	m.matchers.Range(func(key, value interface{}) bool {
		targetGVK := key.(groupVersionKind)
		matchers[targetGVK] = map[enqueueTarget]map[string]objectMatcher{}
		value.(*sync.Map).Range(func(key, value any) bool {
			target := key.(enqueueTarget)
			matchers[targetGVK][target] = map[string]objectMatcher{}
			value.(*sync.Map).Range(func(key, value any) bool {
				mt := value.(objectMatcher)
				matchers[targetGVK][target][mt.String()] = mt
				return true
			})
			return true
		})
		return true
	})

	if !indent {
		return json.Marshal(matchers)
	}

	return json.MarshalIndent(matchers, "", "  ")
}

type groupVersionKind struct {
	schema.GroupVersionKind
}

func (gvk groupVersionKind) MarshalText() ([]byte, error) {
	return []byte(gvk.String()), nil
}
