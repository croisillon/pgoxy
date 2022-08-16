package main

import (
	"flag"
	"io"
	"log"
	"net"
	"os"

	"github.com/diolavr/pgoxy/internal/proxy"
)

var options struct {
	listen   string
	upstream string
}

func main() {
	flag.StringVar(&options.listen, "listen", "0.0.0.0:6432", "Listen address")
	flag.StringVar(&options.upstream, "upstream", "127.0.0.1:5432", "Upstream postgres server")
	flag.Parse()

	ln, err := net.Listen("tcp", options.listen)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Listening on ", ln.Addr())

	defer ln.Close()

	for {
		clientConn, err := ln.Accept()
		if err != nil {
			log.Printf("%v", err)
			continue
		}

		log.Println("Connection from ", clientConn.RemoteAddr())

		go func(cl net.Conn) {

			// Postgres
			serverConn, err := net.Dial("tcp", options.upstream)
			if err != nil {
				log.Printf("%v", err)
				os.Exit(1)
			}
			defer serverConn.Close()

			back := proxy.NewBackend(clientConn)
			front := proxy.NewFrontend(serverConn)

			defer back.Close()
			defer front.Close()

			errorCh := make(chan error)

			// Redirect messages from backend (client) to frontend (server)
			go func(b *proxy.Backend, f *proxy.Frontend, ch chan<- error) {
				for {
					if err := b.RunProxy(f); err != nil {
						if err == io.ErrUnexpectedEOF {
							ch <- nil
							return
						}
						ch <- err
					}
				}
			}(back, front, errorCh)

			// Redirect messages from frontend (server) to backend (client)
			go func(b *proxy.Backend, f *proxy.Frontend, ch chan<- error) {
				for {
					if err := f.RunProxy(b); err != nil {
						if err == io.ErrUnexpectedEOF {
							ch <- nil
							return
						}
						ch <- err
					}
				}
			}(back, front, errorCh)

			err = <-errorCh
			if err != nil {
				log.Printf("%v\n", err)
			}
		}(clientConn)
	}
}
