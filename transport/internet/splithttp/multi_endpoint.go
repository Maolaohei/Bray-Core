package splithttp

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"time"
)

// MultiEndpointRaceWindow is the head-start style window between racing dials.
// Kept short to bound extra connection pressure.
var MultiEndpointRaceWindow = 50 * time.Millisecond

// MultiEndpointProbeTimeout caps a single dual-path probe selection.
var MultiEndpointProbeTimeout = 1500 * time.Millisecond

// MaxMultiEndpoints caps primary+extras in a race list (green-zone).
// Prevents misconfigured x-bray-endpoints from becoming a dial scan.
const MaxMultiEndpoints = 4

// ErrNoMultiEndpoints is returned when RaceDialEndpoints is called with an empty list.
var ErrNoMultiEndpoints = errors.New("xhttp: multi-endpoint race list is empty")

// ErrNilMultiEndpointDial is returned when dialFn is nil.
var ErrNilMultiEndpointDial = errors.New("xhttp: multi-endpoint dial function is nil")

// ErrNilMultiEndpointConn is returned when dialFn succeeds with a nil conn.
var ErrNilMultiEndpointConn = errors.New("xhttp: multi-endpoint dial returned nil conn")

// MultiEndpointDialFunc dials one candidate endpoint.
// Implementations should honor ctx cancellation.
type MultiEndpointDialFunc func(ctx context.Context, endpoint string) (net.Conn, error)

// MultiEndpointEnabled is opt-in via headers["x-bray-multi-endpoint"]=true/1/on/yes.
func MultiEndpointEnabled(headers map[string]string) bool {
	if headers == nil {
		return false
	}
	for k, v := range headers {
		if strings.EqualFold(k, "x-bray-multi-endpoint") {
			switch strings.ToLower(strings.TrimSpace(v)) {
			case "1", "true", "yes", "on":
				return true
			}
		}
	}
	return false
}

// ParseExtraEndpoints reads comma/space-separated endpoints from
// headers["x-bray-endpoints"]. Empty values are ignored.
func ParseExtraEndpoints(headers map[string]string) []string {
	if headers == nil {
		return nil
	}
	var raw string
	for k, v := range headers {
		if strings.EqualFold(k, "x-bray-endpoints") {
			raw = v
			break
		}
	}
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ' ' || r == ';' || r == '\n' || r == '\t'
	})
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	// Cap extras alone so primary+extras stays within MaxMultiEndpoints.
	maxExtras := MaxMultiEndpoints - 1
	if maxExtras < 1 {
		maxExtras = 1
	}
	for _, p := range parts {
		if len(out) >= maxExtras {
			break
		}
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

// RaceDialEndpoints races dialFn across endpoints (primary first). The first
// successful dial wins; losers are closed. If endpoints is empty/nil, returns
// ErrNoMultiEndpoints. If only one endpoint is provided, dials it directly without race.
//
// Compatibility: default single-dest dial path is unchanged; callers must pass
// opt-in endpoints explicitly.
func RaceDialEndpoints(ctx context.Context, endpoints []string, dialFn MultiEndpointDialFunc) (net.Conn, string, error) {
	if len(endpoints) == 0 {
		return nil, "", ErrNoMultiEndpoints
	}
	if dialFn == nil {
		return nil, "", ErrNilMultiEndpointDial
	}
	if len(endpoints) == 1 {
		c, err := dialFn(ctx, endpoints[0])
		if err == nil && c == nil {
			return nil, "", ErrNilMultiEndpointConn
		}
		return c, endpoints[0], err
	}

	ctx, cancel := context.WithTimeout(ctx, MultiEndpointProbeTimeout)
	defer cancel()

	type result struct {
		conn net.Conn
		ep   string
		err  error
	}
	ch := make(chan result, len(endpoints))
	var wg sync.WaitGroup

	for i, ep := range endpoints {
		i, ep := i, ep
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i > 0 {
				timer := time.NewTimer(time.Duration(i) * MultiEndpointRaceWindow)
				defer timer.Stop()
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				}
			}
			if ctx.Err() != nil {
				return
			}
			c, err := dialFn(ctx, ep)
			select {
			case ch <- result{conn: c, ep: ep, err: err}:
			default:
				if c != nil {
					_ = c.Close()
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	var firstErr error
	for r := range ch {
		// treat nil conn as failure even if dialFn returned err==nil
		if r.err == nil && r.conn == nil {
			r.err = ErrNilMultiEndpointConn
		}
		if r.err == nil && r.conn != nil {
			go func(winner string) {
				for late := range ch {
					if late.conn != nil && late.ep != winner {
						_ = late.conn.Close()
					}
				}
			}(r.ep)
			return r.conn, r.ep, nil
		}
		if firstErr == nil && r.err != nil {
			firstErr = r.err
		}
	}
	if firstErr == nil {
		firstErr = ctx.Err()
	}
	if firstErr == nil {
		firstErr = context.DeadlineExceeded
	}
	return nil, "", firstErr
}

// BuildEndpointList returns primary + extras (deduped, primary first).
// primary may be empty; extras alone are still returned.
func BuildEndpointList(primary string, extras []string) []string {
	out := make([]string, 0, 1+len(extras))
	seen := make(map[string]struct{}, 1+len(extras))
	add := func(ep string) {
		if len(out) >= MaxMultiEndpoints {
			return
		}
		ep = strings.TrimSpace(ep)
		if ep == "" {
			return
		}
		if _, ok := seen[ep]; ok {
			return
		}
		seen[ep] = struct{}{}
		out = append(out, ep)
	}
	add(primary)
	for _, ep := range extras {
		add(ep)
	}
	return out
}
