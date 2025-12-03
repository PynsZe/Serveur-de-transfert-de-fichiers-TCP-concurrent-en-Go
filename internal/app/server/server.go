package server

import (
	"log/slog"
	"net"
	"time"
)

func RunServer(port *string) {

	l, e := net.Listen("tcp", ":"+*port)
	if e != nil {
		slog.Error(e.Error())
		return
	}
	defer func() {
		l.Close()
		slog.Debug("Stopped listening on port " + *port)
	}()
	slog.Debug("Now listening on port " + *port)

	c, e := l.Accept()
	if e != nil {
		slog.Error(e.Error())
		return
	}
	defer func() {
		c.Close()
		slog.Info("Connection closed")
	}()
	slog.Info("Incoming connection from " + c.RemoteAddr().String())

	time.Sleep(10 * time.Second)

	return
}
