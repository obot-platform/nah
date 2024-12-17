package router

import (
	"reflect"

	"github.com/obot-platform/nah/pkg/backend"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type save struct {
	cache  backend.CacheFactory
	client kclient.Client
}

func (s *save) save(unmodified runtime.Object, req Request) (kclient.Object, error) {
	newObj := req.Object
	if newObj != nil && StatusChanged(unmodified, newObj) {
		if unmodObj, ok := unmodified.(kclient.Object); ok && unmodObj.GetGeneration() != newObj.GetGeneration() {
			if err := req.Get(unmodObj, unmodObj.GetNamespace(), unmodObj.GetName()); err != nil {
				return nil, err
			}
			unmodified = unmodObj
			if !StatusChanged(unmodified, newObj) {
				return newObj, nil
			}
		}
		return newObj, s.client.Status().Update(req.Ctx, newObj)
	}

	return newObj, nil
}

func statusField(obj runtime.Object) interface{} {
	v := reflect.ValueOf(obj).Elem()
	fieldValue := v.FieldByName("Status")
	if fieldValue.Kind() == reflect.Invalid {
		return nil
	}
	return fieldValue.Interface()
}

func StatusChanged(unmodified, newObj runtime.Object) bool {
	return !equality.Semantic.DeepEqual(statusField(unmodified), statusField(newObj))
}
