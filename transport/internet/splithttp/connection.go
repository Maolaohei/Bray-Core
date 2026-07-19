package splithttp

import (
	"io"
	"net"
	"sync"
	"time"
)

// timeoutError is returned when a splitConn deadline elapses. It implements
// net.Error so callers can detect Timeout() the same way as net.Conn.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

var errTimeout error = timeoutError{}

type ioResult struct {
	n    int
	err  error
	data []byte
}

type splitConn struct {
	writer     io.WriteCloser
	reader     io.ReadCloser
	remoteAddr net.Addr
	localAddr  net.Addr
	onClose    func()

	// Deadlines for sides that are not net.Conn (H2 response bodies, pipes).
	// net.Conn sides still get the deadline applied via Set*Deadline.
	deadlineMu    sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time

	// Serialize deadline-assisted I/O. A timed-out Read may leave an underlying
	// read in flight; the result is parked in pendingRead and delivered next.
	readMu       sync.Mutex
	writeMu      sync.Mutex
	pendingRead  <-chan ioResult
	pendingWrite <-chan ioResult
}

func (c *splitConn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.pendingWrite != nil {
		return c.awaitWrite(b)
	}

	if wd, ok := c.writer.(interface{ SetWriteDeadline(time.Time) error }); ok {
		c.deadlineMu.Lock()
		dl := c.writeDeadline
		c.deadlineMu.Unlock()
		_ = wd.SetWriteDeadline(dl)
		return c.writer.Write(b)
	}

	c.deadlineMu.Lock()
	dl := c.writeDeadline
	c.deadlineMu.Unlock()
	if dl.IsZero() {
		return c.writer.Write(b)
	}
	rem := time.Until(dl)
	if rem <= 0 {
		return 0, errTimeout
	}

	// Copy so the caller's buffer is not raced if we time out mid-write.
	payload := make([]byte, len(b))
	copy(payload, b)
	ch := make(chan ioResult, 1)
	go func() {
		n, err := c.writer.Write(payload)
		ch <- ioResult{n: n, err: err}
	}()

	timer := time.NewTimer(rem)
	defer timer.Stop()
	select {
	case r := <-ch:
		return r.n, r.err
	case <-timer.C:
		c.pendingWrite = ch
		return 0, errTimeout
	}
}

func (c *splitConn) awaitWrite(b []byte) (int, error) {
	ch := c.pendingWrite
	c.deadlineMu.Lock()
	dl := c.writeDeadline
	c.deadlineMu.Unlock()

	var timeout <-chan time.Time
	if !dl.IsZero() {
		rem := time.Until(dl)
		if rem <= 0 {
			return 0, errTimeout
		}
		timer := time.NewTimer(rem)
		defer timer.Stop()
		timeout = timer.C
	}

	select {
	case r := <-ch:
		c.pendingWrite = nil
		// Prior write completed; report that result (do not re-write b).
		return r.n, r.err
	case <-timeout:
		return 0, errTimeout
	}
}

func (c *splitConn) Read(b []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	if c.pendingRead != nil {
		return c.awaitRead(b)
	}

	if rd, ok := c.reader.(interface{ SetReadDeadline(time.Time) error }); ok {
		c.deadlineMu.Lock()
		dl := c.readDeadline
		c.deadlineMu.Unlock()
		_ = rd.SetReadDeadline(dl)
		return c.reader.Read(b)
	}

	c.deadlineMu.Lock()
	dl := c.readDeadline
	c.deadlineMu.Unlock()
	if dl.IsZero() {
		return c.reader.Read(b)
	}
	rem := time.Until(dl)
	if rem <= 0 {
		return 0, errTimeout
	}

	// Intermediate buffer: on timeout the in-flight Read must not write into b.
	buf := make([]byte, len(b))
	ch := make(chan ioResult, 1)
	go func() {
		n, err := c.reader.Read(buf)
		var data []byte
		if n > 0 {
			data = buf[:n]
		}
		ch <- ioResult{n: n, err: err, data: data}
	}()

	timer := time.NewTimer(rem)
	defer timer.Stop()
	select {
	case r := <-ch:
		if r.n > 0 {
			copy(b, r.data)
		}
		return r.n, r.err
	case <-timer.C:
		c.pendingRead = ch
		return 0, errTimeout
	}
}

func (c *splitConn) awaitRead(b []byte) (int, error) {
	ch := c.pendingRead
	c.deadlineMu.Lock()
	dl := c.readDeadline
	c.deadlineMu.Unlock()

	var timeout <-chan time.Time
	if !dl.IsZero() {
		rem := time.Until(dl)
		if rem <= 0 {
			return 0, errTimeout
		}
		timer := time.NewTimer(rem)
		defer timer.Stop()
		timeout = timer.C
	}

	select {
	case r := <-ch:
		c.pendingRead = nil
		if r.n > 0 {
			n := copy(b, r.data)
			if n < r.n {
				// Caller buffer smaller than pending; re-park remainder.
				rest := make(chan ioResult, 1)
				rest <- ioResult{n: r.n - n, err: r.err, data: r.data[n:]}
				c.pendingRead = rest
				return n, nil
			}
		}
		return r.n, r.err
	case <-timeout:
		return 0, errTimeout
	}
}

func (c *splitConn) Close() error {
	var err, err2 error
	if c.writer != nil {
		err = c.writer.Close()
	}
	if c.reader != nil {
		err2 = c.reader.Close()
	}
	if c.onClose != nil {
		c.onClose()
	}
	if err != nil {
		return err
	}
	if err2 != nil {
		return err2
	}
	return nil
}

func (c *splitConn) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *splitConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *splitConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

func (c *splitConn) SetReadDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.readDeadline = t
	c.deadlineMu.Unlock()
	if d, ok := c.reader.(interface{ SetReadDeadline(time.Time) error }); ok {
		return d.SetReadDeadline(t)
	}
	return nil
}

func (c *splitConn) SetWriteDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.writeDeadline = t
	c.deadlineMu.Unlock()
	if d, ok := c.writer.(interface{ SetWriteDeadline(time.Time) error }); ok {
		return d.SetWriteDeadline(t)
	}
	return nil
}
