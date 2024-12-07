package router

import (
	"time"
)

type ResponseWrapper struct {
	Delay time.Duration
	Attr  map[string]any
}

func (r *ResponseWrapper) Attributes() map[string]any {
	if r.Attr == nil {
		r.Attr = map[string]any{}
	}
	return r.Attr
}

func (r *ResponseWrapper) RetryAfter(delay time.Duration) {
	r.Delay = delay
}
