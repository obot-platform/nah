package leader

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

func TestJSONClientConfigForcesJSONContentNegotiation(t *testing.T) {
	original := &rest.Config{
		Host: "https://example.invalid",
		ContentConfig: rest.ContentConfig{
			AcceptContentTypes: "application/vnd.kubernetes.protobuf,application/json",
			ContentType:        "application/vnd.kubernetes.protobuf",
		},
	}

	cfg := jsonClientConfig(original)

	if cfg == original {
		t.Fatal("expected a copied config")
	}
	if cfg.AcceptContentTypes != runtime.ContentTypeJSON {
		t.Fatalf("expected accept content types %q, got %q", runtime.ContentTypeJSON, cfg.AcceptContentTypes)
	}
	if cfg.ContentType != runtime.ContentTypeJSON {
		t.Fatalf("expected content type %q, got %q", runtime.ContentTypeJSON, cfg.ContentType)
	}
	if original.AcceptContentTypes != "application/vnd.kubernetes.protobuf,application/json" {
		t.Fatalf("expected original accept content types to remain unchanged, got %q", original.AcceptContentTypes)
	}
	if original.ContentType != "application/vnd.kubernetes.protobuf" {
		t.Fatalf("expected original content type to remain unchanged, got %q", original.ContentType)
	}
}
