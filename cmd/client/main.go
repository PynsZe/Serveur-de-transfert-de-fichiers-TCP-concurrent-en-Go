package main

import (
	"flag"
	"log/slog"
	"time"

	"gitlab.univ-nantes.fr/iutna.info2.r305/proj/internal/app/client"
)

func parseArgs() (remote string, dir *string, timeout *int) {
	dFlag := flag.Bool("d", false, "enable debug log level")
	aFlag := flag.String("a", "127.0.0.1", "server address (default: 127.0.0.1)")
	pFlag := flag.String("p", "3333", "server port (default: 3333)")
	dir = flag.String("dir", ".", "directory with files to serve")
	timeout = flag.Int("t", 5, "time limit")
	flag.Parse()

	if *dFlag {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	remote = *aFlag + ":" + *pFlag
	return
}

func main() {
	remote, dir, timeout := parseArgs()
	client.Run(remote, dir, time.Duration(*timeout)*time.Second)
}
