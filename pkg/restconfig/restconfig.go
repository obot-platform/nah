package restconfig

import (
	"net/url"
	"os"

	"github.com/obot-platform/nah/pkg/ratelimit"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

func Default() (*rest.Config, error) {
	return New(scheme.Scheme)
}

func ClientConfigFromFile(file, context string) clientcmd.ClientConfig {
	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	loader.ExplicitPath = file
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loader,
		&clientcmd.ConfigOverrides{
			CurrentContext: context,
		})
}

func FromFile(file, context string) (*rest.Config, error) {
	return ClientConfigFromFile(file, context).ClientConfig()
}

func SetScheme(cfg *rest.Config, scheme *runtime.Scheme) *rest.Config {
	cfg.NegotiatedSerializer = serializer.NewCodecFactory(scheme)
	cfg.UserAgent = rest.DefaultKubernetesUserAgent()
	return cfg
}

func New(scheme *runtime.Scheme) (*rest.Config, error) {
	cfg, err := config.GetConfigWithContext(os.Getenv("CONTEXT"))
	if err != nil {
		return nil, err
	}
	cfg.RateLimiter = ratelimit.None
	return SetScheme(cfg, scheme), nil
}

func FromURLTokenAndScheme(serverURL, token string, scheme *runtime.Scheme) (*rest.Config, error) {
	u, err := url.Parse(serverURL)
	if err != nil {
		return nil, err
	}
	insecure := false
	if u.Scheme == "https" && u.Host == "localhost" {
		insecure = true
	}

	cfg := &rest.Config{
		Host: serverURL,
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: insecure,
		},
		BearerToken: token,
		RateLimiter: ratelimit.None,
	}

	SetScheme(cfg, scheme)
	return cfg, nil
}
