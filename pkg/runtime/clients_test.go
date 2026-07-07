package runtime

import (
	"testing"

	"k8s.io/client-go/util/watchlist"
)

func TestUnsupportedWatchListSemanticsListerWatcher(t *testing.T) {
	if !watchlist.DoesClientNotSupportWatchListSemantics(unsupportedWatchListSemanticsListerWatcher{}) {
		t.Fatal("expected lister watcher to opt out of watch-list semantics")
	}
}
