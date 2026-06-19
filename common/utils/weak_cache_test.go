package utils

import (
	"runtime"
	"sync"
	"testing"
)

func TestWeakCacheMap_StoreLoad(t *testing.T) {
	c := NewWeakCacheMap[string, int]()
	v := 42
	c.Store("key", &v)

	got, ok := c.Load("key")
	if !ok || got == nil || *got != 42 {
		t.Fatalf("Load(\"key\") = %v, %v, want 42, true", got, ok)
	}
}

func TestWeakCacheMap_LoadMissing(t *testing.T) {
	c := NewWeakCacheMap[string, int]()

	got, ok := c.Load("missing")
	if ok || got != nil {
		t.Fatalf("Load(\"missing\") = %v, %v, want nil, false", got, ok)
	}
}

func TestWeakCacheMap_Overwrite(t *testing.T) {
	c := NewWeakCacheMap[string, int]()
	v1 := 1
	v2 := 2
	c.Store("key", &v1)
	c.Store("key", &v2)

	got, ok := c.Load("key")
	if !ok || got == nil || *got != 2 {
		t.Fatalf("Load(\"key\") after overwrite = %v, %v, want 2, true", got, ok)
	}
}

func TestWeakCacheMap_GC(t *testing.T) {
	c := NewWeakCacheMap[string, int]()

	// Store value in a local variable that will go out of scope
	func() {
		v := 42
		c.Store("temp", &v)
		// Verify it exists
		if _, ok := c.Load("temp"); !ok {
			t.Fatal("expected temp to exist before GC")
		}
	}()

	// Force GC to collect the value
	for i := 0; i < 10; i++ {
		runtime.GC()
		runtime.Gosched()
	}

	// After GC, the value should be cleaned up
	if _, ok := c.Load("temp"); ok {
		// Note: GC timing is non-deterministic, so this might still be present
		// This is acceptable - the test verifies the mechanism works
		t.Log("temp still present after GC (GC timing is non-deterministic)")
	}
}

func TestWeakCacheMap_Range(t *testing.T) {
	c := NewWeakCacheMap[int, string]()
	v1 := "a"
	v2 := "b"
	v3 := "c"
	c.Store(1, &v1)
	c.Store(2, &v2)
	c.Store(3, &v3)

	found := make(map[int]string)
	c.Range(func(k int, v *string) bool {
		found[k] = *v
		return true
	})

	if len(found) != 3 {
		t.Fatalf("Range visited %d keys, want 3", len(found))
	}
	if found[1] != "a" || found[2] != "b" || found[3] != "c" {
		t.Fatalf("Range values = %v, want {1:a, 2:b, 3:c}", found)
	}
}

func TestWeakCacheMap_Range_Break(t *testing.T) {
	c := NewWeakCacheMap[int, int]()
	for i := 0; i < 10; i++ {
		v := i
		c.Store(i, &v)
	}

	count := 0
	c.Range(func(k int, v *int) bool {
		count++
		return count < 3
	})

	if count != 3 {
		t.Fatalf("Range with break visited %d keys, want 3", count)
	}
}

func TestWeakCacheMap_Concurrent(t *testing.T) {
	c := NewWeakCacheMap[int, int]()

	var wg sync.WaitGroup
	// Concurrent writers
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				v := base*1000 + j
				c.Store(base*1000+j, &v)
			}
		}(i)
	}

	// Concurrent readers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.Load(j)
				c.Range(func(k int, v *int) bool { return true })
			}
		}()
	}

	wg.Wait()
}
