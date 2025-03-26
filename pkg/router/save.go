package router

import (
	"reflect"

	"github.com/obot-platform/nah/pkg/backend"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type save struct {
	cache  backend.CacheFactory
	client kclient.Client
}

func (s *save) save(unmodified runtime.Object, req Request) (kclient.Object, error) {
	ctx, span := tracer.Start(req.Ctx, "save", trace.WithAttributes(attribute.String("key", req.Key), attribute.String("gvk", req.GVK.String())))
	defer span.End()

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
		return newObj, s.client.Status().Update(ctx, newObj)
	}

	return newObj, nil
}

func statusField(obj runtime.Object) any {
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
