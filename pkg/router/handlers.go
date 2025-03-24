package router

import (
	"sync"

	"github.com/obot-platform/nah/pkg/merr"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type handlers struct {
	lock     sync.RWMutex
	handlers map[schema.GroupVersionKind][]handler
}

func (h *handlers) GVKs() (result []schema.GroupVersionKind) {
	for gvk := range h.handlers {
		result = append(result, gvk)
	}
	return result
}

func (h *handlers) AddHandler(name string, gvk schema.GroupVersionKind, hd Handler) {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.handlers[gvk] = append(h.handlers[gvk], handler{name: name, h: hd})
}

func (h *handlers) Handles(req Request) bool {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return len(h.handlers[req.GVK]) > 0
}

func (h *handlers) Handle(req Request, resp *response) error {
	h.lock.RLock()
	var (
		errs     []error
		handlers = h.handlers[req.GVK]
	)
	h.lock.RUnlock()

	for _, handler := range handlers {
		err := handler.handle(req, resp)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return merr.NewErrors(errs...)
}

type handler struct {
	name string
	h    Handler
}

func (h *handler) handle(req Request, resp *response) error {
	ctx, span := tracer.Start(req.Ctx, "handlerSetHandle", trace.WithAttributes(
		attribute.String("gvk", req.GVK.String()),
		attribute.String("namespace", req.Namespace),
		attribute.String("name", req.Name),
		attribute.String("key", req.Key),
		attribute.String("handler", h.name),
	))
	defer span.End()
	return h.h.Handle(req.WithContext(ctx), resp)
}
