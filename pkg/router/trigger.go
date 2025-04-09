package router

import (
	"encoding/json"
	"strings"
	"sync"

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
	matchers  sync.Map
	trigger   backend.Trigger
	gvkLookup backend.Backend
	scheme    *runtime.Scheme
	watcher   watcher
}

type watcher interface {
	WatchGVK(gvks ...schema.GroupVersionKind) error
}

type enqueueTarget struct {
	key string
	gvk schema.GroupVersionKind
}

func (et enqueueTarget) MarshalText() ([]byte, error) {
	return []byte(et.gvk.String() + ": " + et.key), nil
}

func (m *triggers) invokeTriggers(req Request) {
	matchers, _ := m.matchers.LoadOrStore(groupVersionKind{req.GVK}, &sync.Map{})

	matchers.(*sync.Map).Range(func(key, value interface{}) bool {
		et := key.(enqueueTarget)
		if et.gvk == req.GVK &&
			et.key == req.Key {
			return true
		}
		value.(*sync.Map).Range(func(_, value any) bool {
			mt := value.(objectMatcher)
			if mt.Match(req.Namespace, req.Name, req.Object) {
				log.Debugf("Triggering [%s] [%v] from [%s] [%v]", et.key, et.gvk, req.Key, req.GVK)
				_ = m.trigger.Trigger(req.Ctx, et.gvk, et.key, 0)
				return false
			}
			return true
		})

		return true
	})
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
	m.invokeTriggers(req)
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

// UnregisterAndTrigger will unregister all triggers for the object, both as source and target.
// If a trigger source matches the object exactly, then the trigger will be invoked.
func (m *triggers) UnregisterAndTrigger(req Request) {
	deleteMatcher := objectMatcher{Namespace: req.Namespace, Name: req.Name}
	deleteKey := deleteMatcher.String()

	m.matchers.Range(func(key, value interface{}) bool {
		targetGVK := key.(groupVersionKind)
		matchers := value.(*sync.Map)

		// Delete self
		matchers.Delete(enqueueTarget{
			key: req.Key,
			gvk: req.GVK,
		})

		matchers.Range(func(key, value any) bool {
			target := key.(enqueueTarget)
			value.(*sync.Map).Delete(deleteKey)

			value.(*sync.Map).Range(func(_, value any) bool {
				mt := value.(objectMatcher)
				if targetGVK.GroupVersionKind == req.GVK && mt.Match(req.Namespace, req.Name, req.Object) {
					log.Debugf("Triggering [%s] [%v] from [%s] [%v] on delete", target.key, target.gvk, req.Key, req.GVK)
					_ = m.trigger.Trigger(req.Ctx, target.gvk, target.key, 0)
				}
				return true
			})

			return true
		})

		return true
	})
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
