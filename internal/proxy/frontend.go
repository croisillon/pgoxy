package proxy

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/jackc/pgproto3/v2"
)

type Frontend struct {
	conn net.Conn
	*pgproto3.Frontend
}

func (f *Frontend) Read() (pgproto3.BackendMessage, error) {
	return f.Receive()
}

func (f *Frontend) Write(msg pgproto3.FrontendMessage) error {
	return f.Send(msg)
}

func (f *Frontend) Close() error {
	log.Printf("Frontend connection was closed %v", f.conn.RemoteAddr())
	return f.conn.Close()
}

func (f *Frontend) RunProxy(backend *Backend) error {
	msg, err := f.Read()
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			return io.ErrUnexpectedEOF
		}
		return fmt.Errorf("error frontend read: %w", err)
	}

	err = backend.Write(msg)
	if err != nil {
		return fmt.Errorf("error backend write: %w", err)
	}

	return nil
}

func NewFrontend(conn net.Conn) *Frontend {
	return &Frontend{
		conn:     conn,
		Frontend: pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn),
	}
}
