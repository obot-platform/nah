package nah

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/obot-platform/nah/pkg/backend"
	"github.com/obot-platform/nah/pkg/leader"
	"github.com/obot-platform/nah/pkg/logrus"
	"github.com/obot-platform/nah/pkg/restconfig"
	"github.com/obot-platform/nah/pkg/router"
	nruntime "github.com/obot-platform/nah/pkg/runtime"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

const defaultHealthzPort = 8888

type Options struct {
	// If the backend is nil, then DefaultRESTConfig, DefaultNamespace, and Scheme are used to create a backend.
	Backend backend.Backend
	// If a Backend is provided, then this is ignored. If not provided and needed, then a default is created with Scheme.
	DefaultRESTConfig *rest.Config
	// If a Backend is provided, then this is ignored.
	DefaultNamespace string
	// If a Backend is provided, then this is ignored.
	Scheme *runtime.Scheme
	// APIGroupConfigs are keyed by an API group. This indicates to the router that all actions on this group should use the
	// given Config. This is useful for routers that watch different objects on different API servers.
	APIGroupConfigs map[string]nruntime.GroupConfig
	// ElectionConfig being nil represents no leader election for the router.
	ElectionConfig *leader.ElectionConfig
	// Defaults to 8888
	HealthzPort int
	// DSN to use for persistent store
	DSN string
	// Change the threadedness per GVK
	GVKThreadiness map[schema.GroupVersionKind]int

	db *sql.DB
}

func (o *Options) complete(handlerName string) (*Options, error) {
	var (
		result Options
		err    error
	)
	if o != nil {
		result = *o
	}

	if result.Scheme == nil {
		return nil, fmt.Errorf("scheme is required to be set")
	}

	if result.HealthzPort == 0 {
		result.HealthzPort = defaultHealthzPort
	}

	if result.Backend != nil {
		return &result, nil
	}

	if result.DefaultRESTConfig == nil {
		result.DefaultRESTConfig, err = restconfig.New(result.Scheme)
		if err != nil {
			return nil, err
		}
	}

	if result.DSN != "" {
		result.db, err = newDB(result.DSN)
		if err != nil {
			return nil, fmt.Errorf("error creating database connection: %v", err)
		}
	}

	defaultConfig := nruntime.Config{
		GroupConfig: nruntime.GroupConfig{
			Rest:      result.DefaultRESTConfig,
			Namespace: result.DefaultNamespace,
			DB:        result.db,
		},
	}
	backend, err := nruntime.NewRuntimeWithConfigs(handlerName, defaultConfig, result.APIGroupConfigs, result.Scheme)
	if err != nil {
		return nil, err
	}
	result.Backend = backend.Backend

	return &result, nil
}

func newDB(dsn string) (*sql.DB, error) {
	var (
		gdb                    gorm.Dialector
		pool                   bool
		skipDefaultTransaction bool
	)
	switch {
	case strings.HasPrefix(dsn, "sqlite://"):
		skipDefaultTransaction = true
		gdb = sqlite.Open(strings.TrimPrefix(dsn, "sqlite://"))
	case strings.HasPrefix(dsn, "postgresql://"):
		dsn = strings.Replace(dsn, "postgresql://", "postgres://", 1)
		fallthrough
	case strings.HasPrefix(dsn, "postgres://"):
		gdb = postgres.Open(dsn)
		pool = true
	default:
		return nil, fmt.Errorf("unsupported database: %s", dsn)
	}
	db, err := gorm.Open(gdb, &gorm.Config{
		SkipDefaultTransaction: skipDefaultTransaction,
		Logger: logrus.New(logrus.Config{
			SlowThreshold:             200 * time.Millisecond,
			IgnoreRecordNotFoundError: true,
			LogSQL:                    true,
		}),
	})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetConnMaxLifetime(time.Minute * 3)
	if pool {
		sqlDB.SetMaxIdleConns(5)
		sqlDB.SetMaxOpenConns(5)
	} else {
		sqlDB.SetMaxIdleConns(1)
		sqlDB.SetMaxOpenConns(1)
	}

	return sqlDB, nil
}

// DefaultOptions represent the standard options for a Router.
// The default leader election uses a lease lock and a TTL of 15 seconds.
func DefaultOptions(routerName string, scheme *runtime.Scheme) (*Options, error) {
	cfg, err := restconfig.New(scheme)
	if err != nil {
		return nil, err
	}
	rt, err := nruntime.NewRuntimeForNamespace(routerName, cfg, "", scheme)
	if err != nil {
		return nil, err
	}

	return &Options{
		Backend:           rt.Backend,
		DefaultRESTConfig: cfg,
		Scheme:            scheme,
		ElectionConfig:    leader.NewDefaultElectionConfig("", routerName, cfg),
		HealthzPort:       defaultHealthzPort,
	}, nil
}

// DefaultRouter The routerName is important as this name will be used to assign ownership of objects created by this
// router. Specifically, the routerName is assigned to the sub-context in the apply actions. Additionally, the routerName
// will be used for the leader election lease lock.
func DefaultRouter(routerName string, scheme *runtime.Scheme) (*router.Router, error) {
	opts, err := DefaultOptions(routerName, scheme)
	if err != nil {
		return nil, err
	}
	return NewRouter(routerName, opts)
}

func NewRouter(handlerName string, opts *Options) (*router.Router, error) {
	opts, err := opts.complete(handlerName)
	if err != nil {
		return nil, err
	}
	hs, err := router.NewHandlerSet(handlerName, opts.Backend.Scheme(), opts.Backend, opts.db)
	if err != nil {
		return nil, err
	}
	return router.New(hs, opts.ElectionConfig, opts.HealthzPort), nil
}
