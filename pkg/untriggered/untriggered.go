package untriggered

import (
	"k8s.io/apimachinery/pkg/runtime"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type Holder struct {
	kclient.Object
	uncached bool
}

func (h *Holder) DeepCopyObject() runtime.Object {
	return &Holder{Object: h.Object.DeepCopyObject().(kclient.Object), uncached: h.uncached}
}

func (h *Holder) IsUncached() bool {
	return h.uncached
}

type HolderList struct {
	kclient.ObjectList
	uncached bool
}

func (h *HolderList) DeepCopyObject() runtime.Object {
	return &HolderList{ObjectList: h.ObjectList.DeepCopyObject().(kclient.ObjectList), uncached: h.uncached}
}

func (h *HolderList) IsUncached() bool {
	return h.uncached
}

func List(obj kclient.ObjectList) kclient.ObjectList {
	return &HolderList{
		ObjectList: obj,
	}
}

func UncachedList(obj kclient.ObjectList) kclient.ObjectList {
	return &HolderList{
		ObjectList: obj,
		uncached:   true,
	}
}

func Get(obj kclient.Object) kclient.Object {
	return &Holder{
		Object: obj,
	}
}

func UncachedGet(obj kclient.Object) kclient.Object {
	return &Holder{
		Object:   obj,
		uncached: true,
	}
}

func IsWrapped(obj runtime.Object) bool {
	if _, ok := obj.(*Holder); ok {
		return true
	}
	if _, ok := obj.(*HolderList); ok {
		return true
	}
	return false
}

func IsUncached(obj runtime.Object) bool {
	if h, ok := obj.(*Holder); ok {
		return h.uncached
	}
	if h, ok := obj.(*HolderList); ok {
		return h.uncached
	}
	return false
}

func Unwrap(obj runtime.Object) runtime.Object {
	if h, ok := obj.(*Holder); ok {
		return h.Object
	}
	if h, ok := obj.(*HolderList); ok {
		return h.ObjectList
	}
	return obj
}

func UnwrapList(obj kclient.ObjectList) kclient.ObjectList {
	if h, ok := obj.(*HolderList); ok {
		return h.ObjectList
	}
	return obj
}
