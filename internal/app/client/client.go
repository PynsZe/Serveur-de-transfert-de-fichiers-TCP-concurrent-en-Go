package client

import (
	"log/slog"
	"net"
)

func Run(remote string) {

	c, e := net.Dial("tcp", remote)
	if e != nil {
		slog.Error(e.Error())
		return
	}
	defer func() {
		c.Close()
		slog.Debug("Connection closed")
	}()
	slog.Info("Connected to " + c.RemoteAddr().String())

	return
}
