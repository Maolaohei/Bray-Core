package noop

import (
	"context"
	"net"

	"github.com/xtls/xray-core/common"
)

type NoOpConnectionHeader struct{}

func (NoOpConnectionHeader) Client(conn net.Conn) net.Conn {
	return conn
}

func (NoOpConnectionHeader) Server(conn net.Conn) net.Conn {
	return conn
}

func NewNoOpConnectionHeader(context.Context, interface{}) (interface{}, error) {
	return NoOpConnectionHeader{}, nil
}

func init() {
	common.Must(common.RegisterConfig((*ConnectionConfig)(nil), NewNoOpConnectionHeader))
}
