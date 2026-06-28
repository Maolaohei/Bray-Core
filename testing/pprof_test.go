//go:build pprof

package scenarios

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	profileDuration = 60 * time.Second
	concurrency     = 200
	totalRequests   = 50000
)

type ProfileReport struct {
	Duration        time.Duration
	TotalRequests   int64
	SuccessRequests int64
	FailedRequests  int64
	RequestsPerSec  float64
	MemoryAllocMB   float64
	MemoryHeapMB    float64
	GoroutinesStart int
	GoroutinesEnd   int
	GoroutinesPeak  int
}

func TestPProf_Profiling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pprof profiling in short mode")
	}

	t.Log("Starting comprehensive pprof profiling...")
	t.Logf("CPU sampling duration: %v", profileDuration)
	t.Logf("Concurrency: %d", concurrency)
	t.Logf("Total requests: %d", totalRequests)

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	goroutinesBefore := runtime.NumGoroutine()

	ts := time.Now().Format("20060102_150405")
	cpuFile, err := os.Create(fmt.Sprintf("pprof_cpu_%s.prof", ts))
	if err != nil {
		t.Fatalf("Failed to create CPU profile: %v", err)
	}
	defer cpuFile.Close()

	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		t.Fatalf("Failed to start CPU profile: %v", err)
	}

	var goroutinePeak atomic.Int32
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n := int32(runtime.NumGoroutine())
				for {
					old := goroutinePeak.Load()
					if n <= old || goroutinePeak.CompareAndSwap(old, n) {
						break
					}
				}
			case <-done:
				return
			}
		}
	}()

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	var successCount, failCount atomic.Int64
	deadline := time.Now().Add(profileDuration)
	startTime := time.Now()

	for i := 0; i < totalRequests; i++ {
		if time.Now().After(deadline) {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			runWorkload(idx, &successCount, &failCount)
		}(i)
	}
	wg.Wait()
	duration := time.Since(startTime)

	pprof.StopCPUProfile()

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	memFile, err := os.Create(fmt.Sprintf("pprof_mem_%s.prof", ts))
	if err != nil {
		t.Fatalf("Failed to create memory profile: %v", err)
	}
	defer memFile.Close()
	pprof.WriteHeapProfile(memFile)

	close(done)
	goroutinesAfter := runtime.NumGoroutine()

	report := &ProfileReport{
		Duration:        duration,
		TotalRequests:   int64(totalRequests),
		SuccessRequests: successCount.Load(),
		FailedRequests:  failCount.Load(),
		RequestsPerSec:  float64(successCount.Load()) / duration.Seconds(),
		MemoryAllocMB:   float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / 1024 / 1024,
		MemoryHeapMB:    float64(memAfter.HeapAlloc) / 1024 / 1024,
		GoroutinesStart: goroutinesBefore,
		GoroutinesEnd:   goroutinesAfter,
		GoroutinesPeak:  int(goroutinePeak.Load()),
	}

	printPProfReport(t, report, ts)
}

func runWorkload(idx int, success, fail *atomic.Int64) {
	switch idx % 5 {
	case 0:
		runTCPSimWorkload(success, fail)
	case 1:
		runDataProcessingWorkload(success, fail)
	case 2:
		runMemoryAllocWorkload(success, fail)
	case 3:
		runCryptoWorkload(success, fail)
	case 4:
		runGoroutineWorkload(success, fail)
	}
}

func runTCPSimWorkload(success, fail *atomic.Int64) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fail.Add(1)
		return
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		io.Copy(conn, conn)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		fail.Add(1)
		return
	}
	defer conn.Close()

	for i := 0; i < 10; i++ {
		data := make([]byte, 65536)
		rand.Read(data)
		if _, err := conn.Write(data); err != nil {
			fail.Add(1)
			return
		}
		buf := make([]byte, 65536)
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(conn, buf); err != nil {
			fail.Add(1)
			return
		}
	}
	success.Add(1)
}

func runDataProcessingWorkload(success, fail *atomic.Int64) {
	for iter := 0; iter < 100; iter++ {
		data := make([]byte, 16384)
		if _, err := rand.Read(data); err != nil {
			fail.Add(1)
			return
		}

		result := make([]byte, len(data))
		for i, v := range data {
			result[i] = v ^ 0xAA
		}

		m := make(map[string][]byte)
		for i := 0; i < 100; i++ {
			m[fmt.Sprintf("key_%d", i)] = data[i%len(data):]
		}

		ints := make([]int, 1000)
		for i := range ints {
			ints[i] = len(data) - i
		}
		sort.Ints(ints)
		_ = result
		_ = m
	}
	success.Add(1)
}

func runMemoryAllocWorkload(success, fail *atomic.Int64) {
	for iter := 0; iter < 100; iter++ {
		for i := 0; i < 50; i++ {
			size := 128 << (i % 10)
			buf := make([]byte, size)
			rand.Read(buf)
			_ = buf
		}
	}

	pool := sync.Pool{
		New: func() interface{} {
			b := make([]byte, 1024)
			return &b
		},
	}

	for i := 0; i < 10000; i++ {
		bp := pool.Get().(*[]byte)
		*bp = (*bp)[:0]
		pool.Put(bp)
	}

	success.Add(1)
}

func runCryptoWorkload(success, fail *atomic.Int64) {
	for iter := 0; iter < 100; iter++ {
		key := make([]byte, 32)
		rand.Read(key)

		data := make([]byte, 65536)
		rand.Read(data)

		encrypted := make([]byte, len(data))
		for i, v := range data {
			encrypted[i] = v ^ key[i%len(key)]
		}

		decrypted := make([]byte, len(encrypted))
		for i, v := range encrypted {
			decrypted[i] = v ^ key[i%len(key)]
		}

		if !bytes.Equal(data, decrypted) {
			fail.Add(1)
			return
		}

		h := sha256.New()
		h.Write(data)
		_ = h.Sum(nil)
	}
	success.Add(1)
}

func runGoroutineWorkload(success, fail *atomic.Int64) {
	for iter := 0; iter < 100; iter++ {
		var wg sync.WaitGroup
		results := make(chan int, 50)

		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				sum := 0
				for j := 0; j < 10000; j++ {
					sum += j
					buf := make([]byte, 1024)
					binary.LittleEndian.PutUint32(buf, uint32(j))
				}
				results <- sum
			}(i)
		}

		go func() {
			wg.Wait()
			close(results)
		}()

		sum := 0
		for r := range results {
			sum += r
		}
		_ = sum
	}
	success.Add(1)
}

func printPProfReport(t *testing.T, r *ProfileReport, ts string) {
	t.Logf("")
	t.Logf("╔══════════════════════════════════════════════════════╗")
	t.Logf("║           Bray-Core PPROF Profiling Report          ║")
	t.Logf("╠══════════════════════════════════════════════════════╣")
	t.Logf("║ Duration:        %-33v ║", r.Duration.Round(time.Millisecond))
	t.Logf("║ Requests:        %-33v ║", fmt.Sprintf("%d/%d succeeded", r.SuccessRequests, r.TotalRequests))
	t.Logf("║ Throughput:      %-33v ║", fmt.Sprintf("%.0f req/s", r.RequestsPerSec))
	t.Logf("║ Memory Alloc:    %-33v ║", fmt.Sprintf("%.2f MB", r.MemoryAllocMB))
	t.Logf("║ Heap In-Use:     %-33v ║", fmt.Sprintf("%.2f MB", r.MemoryHeapMB))
	t.Logf("║ Goroutines:      %-33v ║", fmt.Sprintf("%d → %d (peak %d)", r.GoroutinesStart, r.GoroutinesEnd, r.GoroutinesPeak))
	t.Logf("╠══════════════════════════════════════════════════════╣")
	t.Logf("║ Profiles written:                                      ║")
	t.Logf("║   CPU: pprof_cpu_%s.prof                ║", ts)
	t.Logf("║   Mem: pprof_mem_%s.prof                ║", ts)
	t.Logf("║                                                        ║")
	t.Logf("║ TOP 10 Analysis:                                       ║")
	t.Logf("║   CPU top10: go tool pprof -top pprof_cpu_%s.prof    ║", ts)
	t.Logf("║   Mem top10: go tool pprof -top pprof_mem_%s.prof    ║", ts)
	t.Logf("║   Call Graph: go tool pprof pprof_cpu_%s.prof        ║", ts)
	t.Logf("║   Interactive: go tool pprof pprof_cpu_%s.prof       ║", ts)
	t.Logf("║     > top 10                                          ║")
	t.Logf("║     > list <function_name>                            ║")
	t.Logf("║     > web  (requires graphviz)                        ║")
	t.Logf("╚══════════════════════════════════════════════════════╝")
	t.Logf("")

	if r.SuccessRequests == 0 {
		t.Error("No requests succeeded")
	}
	if r.FailedRequests > r.TotalRequests/10 {
		t.Errorf("Too many failures: %d/%d", r.FailedRequests, r.TotalRequests)
	}
	if r.GoroutinesPeak > concurrency*3 {
		t.Errorf("Goroutine peak %d exceeds 3x concurrency %d", r.GoroutinesPeak, concurrency)
	}
}
