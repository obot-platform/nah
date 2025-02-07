package triggers

import (
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type objectMatcher struct {
	Namespace string
	Name      string
	Selector  labels.Selector
	Fields    fields.Selector
}

func (o *objectMatcher) string() string {
	s := o.Name + "/" + o.Namespace
	if o.Selector != nil {
		s += "/label selectors" + o.Selector.String()
	}
	if o.Fields != nil {
		s += "/field selectors" + o.Fields.String()
	}
	return s
}

func (o *objectMatcher) match(ns, name string, obj kclient.Object) bool {
	if o.Name != "" {
		return o.Name == name &&
			o.Namespace == ns
	}
	if o.Namespace != "" && o.Namespace != ns {
		return false
	}
	if o.Selector != nil || o.Fields != nil {
		if obj == nil {
			return false
		}
		var (
			selectorMatches = true
			fieldMatches    = true
		)
		if o.Selector != nil {
			selectorMatches = o.Selector.Matches(labels.Set(obj.GetLabels()))
		}
		if o.Fields != nil {
			if i, ok := obj.(fields.Fields); ok {
				fieldMatches = o.Fields.Matches(i)
			}
		}
		return selectorMatches && fieldMatches
	}
	if o.Fields != nil {
		if obj == nil {
			return false
		}
		if i, ok := obj.(fields.Fields); ok {
			return o.Fields.Matches(i)
		}
	}
	return o.Namespace == "" || o.Namespace == ns
}
