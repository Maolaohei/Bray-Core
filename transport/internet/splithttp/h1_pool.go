package splithttp

// defaultH1UploadPoolCap bounds idle H1 upload connections per dialer client.
// Excess Put() closes the connection instead of growing without limit.
const defaultH1UploadPoolCap = 16

// h1ConnPool is a small fixed-capacity idle pool for HTTP/1.1 upload conns.
// Unlike sync.Pool it never retains more than cap connections and does not
// rely on GC to drop entries (failed/closed conns are never returned).
type h1ConnPool struct {
	ch chan *H1Conn
}

func newH1ConnPool(cap int) *h1ConnPool {
	if cap <= 0 {
		cap = defaultH1UploadPoolCap
	}
	return &h1ConnPool{ch: make(chan *H1Conn, cap)}
}

// Get returns an idle conn or nil if the pool is empty.
func (p *h1ConnPool) Get() *H1Conn {
	if p == nil {
		return nil
	}
	select {
	case c := <-p.ch:
		return c
	default:
		return nil
	}
}

// Put stores c when capacity remains; otherwise closes c.
func (p *h1ConnPool) Put(c *H1Conn) {
	if p == nil || c == nil {
		if c != nil {
			_ = c.Close()
		}
		return
	}
	select {
	case p.ch <- c:
	default:
		_ = c.Close()
	}
}
