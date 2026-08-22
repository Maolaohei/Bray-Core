package splithttp

import (
	"math"
	"testing"
)

func TestDownSegJitterDistribution(t *testing.T) {
	const n = 20000
	var sum int64
	over2m, under256k, under512k := 0, 0, 0
	minS, maxS := int64(1<<62), int64(0)
	for i := 0; i < n; i++ {
		s := int64(downsegSize) + int64(downsegSizeJitterFn())
		if s < minS {
			minS = s
		}
		if s > maxS {
			maxS = s
		}
		sum += s
		if s > 2<<20 {
			over2m++
		}
		if s < 256<<10 {
			under256k++
		}
		if s < 512<<10 {
			under512k++
		}
	}
	meanMiB := float64(sum) / float64(n) / (1 << 20)
	t.Logf("mean=%.3fMiB min=%.0fKiB max=%.2fMiB P(>2MiB)=%.3f P(<512K)=%.4f P(<256K)=%.4f",
		meanMiB, float64(minS)/1024, float64(maxS)/(1<<20),
		float64(over2m)/n, float64(under512k)/n, float64(under256k)/n)
	if meanMiB < 0.9 || meanMiB > 1.1 {
		t.Fatalf("mean drifted: %.3f MiB", meanMiB)
	}
	if math.MaxInt32 <= 0 {
		t.Fatal("impossible")
	}
}
