package splithttp

import (
	"bufio"
	"net"
)

type H1Conn struct {
	UnreadResponsesCount int
	RespBufReader        *bufio.Reader
	net.Conn
}

func NewH1Conn(conn net.Conn) *H1Conn {
	return &H1Conn{
		RespBufReader: bufio.NewReaderSize(conn, 32*1024),
		Conn:          conn,
	}
}
