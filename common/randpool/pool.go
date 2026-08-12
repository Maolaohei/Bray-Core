package randpool

import (
	crand "crypto/rand"
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"
)

const bufferSize = 128 * 1024

type Pool struct {
	buf        []byte
	offset     atomic.Uint32
	refillLock sync.Mutex
}

var Global Pool

func init() {
	Global.buf = make([]byte, bufferSize)
	Global.refill()
}

func (p *Pool) refill() error {
	_, err := crand.Read(p.buf)
	if err != nil {
		return err
	}
	p.offset.Store(0)
	return nil
}

func (p *Pool) ensure() {
	if p.offset.Load() < bufferSize-4 {
		return
	}
	p.refillLock.Lock()
	defer p.refillLock.Unlock()
	if p.offset.Load() < bufferSize-4 {
		return
	}
	p.refill()
}

func (p *Pool) nextUint32() uint32 {
	for {
		p.ensure()
		old := p.offset.Add(4) - 4
		if old+4 <= bufferSize {
			return binary.LittleEndian.Uint32(p.buf[old : old+4])
		}
	}
}

func (p *Pool) IntN(n int) int {
	if n <= 0 {
		return 0
	}
	un := uint32(n)
	limit := math.MaxUint32 - math.MaxUint32%un
	for {
		v := p.nextUint32()
		if v < limit {
			return int(v % un)
		}
	}
}

// Uint32 returns a uniformly-distributed random uint32 from the pool.
// Cheaper than IntN: no modulo-reduction, no rejection loop — just one
// buffered 4-byte read. Use this when the caller wants a small power-of-two
// range via bitmask (e.g. Uint32() & 3 for a 4-way pick), which compiles
// down to a single AND instruction (~1 cycle) instead of a division
// (~10-15 cycles).
func (p *Pool) Uint32() uint32 {
	return p.nextUint32()
}
