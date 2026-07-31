package stats_test

import (
	"context"
	"sync"
	"testing"

	. "github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/features/stats"
)

func TestStatsCounter(t *testing.T) {
	raw, err := common.CreateObject(context.Background(), &Config{})
	common.Must(err)

	m := raw.(stats.Manager)
	c, err := m.RegisterCounter("test.counter")
	common.Must(err)

	if v := c.Add(1); v != 1 {
		t.Fatal("unexpected Add(1) return: ", v, ", wanted ", 1)
	}

	if v := c.Set(0); v != 1 {
		t.Fatal("unexpected Set(0) return: ", v, ", wanted ", 1)
	}

	if v := c.Value(); v != 0 {
		t.Fatal("unexpected Value() return: ", v, ", wanted ", 0)
	}
}

func TestGetOrRegisterCounterConcurrent(t *testing.T) {
	// Concurrent GetOrRegister must never return the "already registered"
	// error and must yield one shared instance (#6468).
	raw, err := common.CreateObject(context.Background(), &Config{})
	common.Must(err)
	m := raw.(stats.Manager)

	const workers = 32
	results := make([]stats.Counter, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := m.GetOrRegisterCounter("concurrent.counter")
			results[i], errs[i] = c, err
		}(i)
	}
	wg.Wait()

	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d: GetOrRegisterCounter error: %v", i, errs[i])
		}
		if results[i] == nil {
			t.Fatalf("worker %d: nil counter", i)
		}
		if results[i] != results[0] {
			t.Fatalf("worker %d: got a different instance than worker 0", i)
		}
	}
}
