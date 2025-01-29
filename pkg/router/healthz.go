package router

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/obot-platform/nah/pkg/log"
)

var healthz struct {
	healths map[string]bool
	started bool
	lock    *sync.RWMutex
	port    int
}

func init() {
	healthz.lock = &sync.RWMutex{}
	healthz.healths = make(map[string]bool)
}

func setPort(port int) {
	healthz.lock.Lock()
	defer healthz.lock.Unlock()
	if healthz.port > 0 {
		log.Warnf("healthz port cannot be changed")
		return
	}
	healthz.port = port
}

func setHealthy(name string, healthy bool) {
	healthz.lock.Lock()
	defer healthz.lock.Unlock()
	healthz.healths[name] = healthy
}

func GetHealthy() bool {
	healthz.lock.RLock()
	defer healthz.lock.RUnlock()
	for _, healthy := range healthz.healths {
		if !healthy {
			return false
		}
	}
	return len(healthz.healths) > 0
}

// startHealthz starts a healthz server on the healthzPort. If the server is already running, then this is a no-op.
// Similarly, if the healthzPort is <= 0, then this is a no-op.
func startHealthz(ctx context.Context) {
	healthz.lock.Lock()
	defer healthz.lock.Unlock()
	if healthz.started || healthz.port <= 0 {
		return
	}
	healthz.started = true

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, req *http.Request) {
		if GetHealthy() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", healthz.port),
		Handler: mux,
	}
	context.AfterFunc(ctx, func() {
		if err := srv.Shutdown(context.Background()); err != nil {
			log.Warnf("error shutting down healthz server: %v", err)
		}
	})
	go func() {
		log.Infof("healthz server stopped: %v", srv.ListenAndServe())
	}()
}
