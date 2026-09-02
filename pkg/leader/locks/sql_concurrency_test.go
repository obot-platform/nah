package locks

import (
	"context"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Every method is called from several goroutines at once, on a plain lock and on a
// bridged one. Run under the race detector. Errors (conflicts, lost races) are
// expected and not the point.
func TestSQLLockIsSafeForConcurrentUse(t *testing.T) {
	ctx := context.Background()
	plain, err := NewSQL(ctx, newDB(t), "plain", "me")
	if err != nil {
		t.Fatal(err)
	}
	legacy := &fakeLegacy{}
	legacy.released(time.Now())
	bridged, err := NewSQL(ctx, newDB(t), "bridged", "me", WithLegacyLock(legacy))
	if err != nil {
		t.Fatal(err)
	}
	bridged.(*sqlLock).grace = 0

	for _, l := range []resourcelock.Interface{plain, bridged} {
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					_, _, _ = l.Get(ctx)
					_ = l.Create(ctx, record("me"))
					_ = l.Update(ctx, record("me"))
				}
			}()
		}
		wg.Wait()

		got, _, err := l.Get(ctx)
		if err != nil || got.HolderIdentity != "me" {
			t.Fatalf("%s: after concurrent use holder %q err %v", l.Describe(), got.HolderIdentity, err)
		}
	}
}
