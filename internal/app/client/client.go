package client

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
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


	in := bufio.NewReader(c)
	out := bufio.NewWriter(c)
	stdin := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("commande : ")
		line, _ := stdin.ReadString('\n')
        line = strings.TrimSpace(line)

		_, err := out.WriteString(line + "\n")
		if (err != nil){
			slog.Error(e.Error())
			return
		}
		out.Flush()

		if (line == "end"){
			slog.Info("Session closed by user")
			return
		}

		resp, err := in.ReadString('\n')
		if (err != nil){
			slog.Error(e.Error())
			return
		}

		fmt.Println(resp)
	}
}
