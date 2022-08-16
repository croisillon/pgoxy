package proxy

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/jackc/pgproto3/v2"
)

type Backend struct {
	conn net.Conn
	*pgproto3.Backend
	readStart bool
}

func (b *Backend) Read() (pgproto3.FrontendMessage, error) {
	if !b.readStart {
		msg, err := b.ReceiveStartupMessage()
		if err != nil {
			return nil, err
		}

		switch msg.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			// SSL Session Encryption or GSSAPI Session Encryption
			// https://www.postgresql.org/docs/current/protocol-flow.html
			b.conn.Write([]byte("N"))
			return nil, nil
		case *pgproto3.StartupMessage:
			// Start-up
			// https://www.postgresql.org/docs/current/protocol-flow.html
			b.readStart = true
			return msg, nil
		case *pgproto3.CancelRequest:
			// Canceling Requests in Progress
			// https://www.postgresql.org/docs/current/protocol-flow.html
			return nil, fmt.Errorf("cancel request")
		}
	}

	return b.Receive()
}

func (b *Backend) Write(msg pgproto3.BackendMessage) error {
	return b.Send(msg)
}

func (b *Backend) Close() error {
	log.Printf("Backend connection was closed %v", b.conn.RemoteAddr())
	return b.conn.Close()
}

func (b *Backend) RunProxy(frontend *Frontend) error {
	msg, err := b.Read()
	if msg == nil && err == nil {
		return nil
	}

	if err != nil {
		if err == io.ErrUnexpectedEOF {
			return io.ErrUnexpectedEOF
		}
		return fmt.Errorf("error backend read: %w", err)
	}

	err = frontend.Write(msg)
	if err != nil {
		return fmt.Errorf("error frontend write: %w", err)
	}

	return nil
}

func NewBackend(conn net.Conn) *Backend {
	return &Backend{
		conn:    conn,
		Backend: pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn),
	}
}
