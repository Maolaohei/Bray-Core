package splithttp

import (
	"errors"
	"io"
	"net"
	"time"
)

type splitConn struct {
	writer     io.WriteCloser
	reader     io.ReadCloser
	remoteAddr net.Addr
	localAddr  net.Addr
	onClose    func()
}

func (c *splitConn) Write(b []byte) (int, error) {
	return c.writer.Write(b)
}

func (c *splitConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}

func (c *splitConn) Close() error {
	// Close resources first, then call onClose (resource cleanup is more important)
	err := c.writer.Close()
	err2 := c.reader.Close()
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
	// splitConn bridges a pipe reader/writer — deadlines are not applicable.
	return errors.New("splitConn: deadline not supported")
}

func (c *splitConn) SetReadDeadline(t time.Time) error {
	return errors.New("splitConn: deadline not supported")
}

func (c *splitConn) SetWriteDeadline(t time.Time) error {
	return errors.New("splitConn: deadline not supported")
}
