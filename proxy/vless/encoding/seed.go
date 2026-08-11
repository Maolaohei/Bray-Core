package encoding

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// Seed wire format: 8 bytes = 4-byte big-endian Unix seconds + 4-byte random
// nonce. The client stamps every request header with a fresh seed; the server
// rejects seeds outside the acceptance window and replays within it. This
// closes the "plaintext 17-byte VLESS header can be replayed verbatim" gap
// (P0) without changing the wire format for legacy clients: a nil seed is
// accepted by the server (old clients) and an unknown addons field is ignored
// by old servers.
const (
	seedLength    = 8
	seedWindow    = 90 * time.Second
	seedReplayMax = 65536
)

// NewSeed returns a fresh 8-byte anti-replay seed for a request header.
func NewSeed() []byte {
	seed := make([]byte, seedLength)
	binary.BigEndian.PutUint32(seed[:4], uint32(time.Now().Unix()))
	if _, err := rand.Read(seed[4:]); err != nil {
		// crypto/rand failure is effectively fatal; still fill the nonce so
		// the timestamp alone never becomes the only entropy.
		seed[4] = byte(time.Now().Nanosecond())
	}
	return seed
}

// seedReplay tracks recently accepted seeds (bounded) so an attacker cannot
// replay a captured header within the acceptance window. Sharded by seed hash
// so concurrent header validation (one per new connection) does not serialize
// on a single global mutex; each shard caps at seedReplayMax/shardCount.
const seedReplayShards = 64

type seedReplayShard struct {
	mu   sync.Mutex
	seen map[[seedLength]byte]int64 // seed -> unix seconds
}

var seedReplay = [seedReplayShards]seedReplayShard{}

func seedShardIndex(key *[seedLength]byte) int {
	// Mix both the timestamp half and the random nonce half so seeds with
	// equal seconds (same timestamp, different nonce) still spread across
	// shards.
	h := uint32(0)
	for _, b := range key {
		h = h*31 + uint32(b)
	}
	return int(h % seedReplayShards)
}

// ValidateSeed checks length, timestamp window and replay status. A nil seed
// is accepted (legacy clients never sent one).
func ValidateSeed(seed []byte) error {
	if seed == nil {
		return nil
	}
	if len(seed) != seedLength {
		return errors.New("invalid seed length: ", len(seed))
	}
	var key [seedLength]byte
	copy(key[:], seed)
	ts := int64(binary.BigEndian.Uint32(seed[:4]))
	now := time.Now().Unix()
	window := int64(seedWindow.Seconds())
	if ts > now+window || now-ts > window {
		return errors.New("seed timestamp out of window")
	}

	shard := &seedReplay[seedShardIndex(&key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.seen == nil {
		shard.seen = make(map[[seedLength]byte]int64)
	}
	if _, dup := shard.seen[key]; dup {
		return errors.New("seed replay detected")
	}
	perShard := seedReplayMax / seedReplayShards
	if len(shard.seen) >= perShard {
		// Capacity bound: drop expired entries (usually all of them).
		for k, v := range shard.seen {
			if now-v > window {
				delete(shard.seen, k)
			}
		}
		if len(shard.seen) >= perShard {
			return errors.New("seed replay map full")
		}
	}
	shard.seen[key] = now
	return nil
}
