package main

import (
	"flag"
	"log/slog"

	"gitlab.univ-nantes.fr/iutna.info2.r305/proj/internal/app/server"
)

func parseArgs() (port *string, dir *string) {

	logLevel := flag.Bool("d", false, "enable debug log level")
	port = flag.String("p", "3333", "server port (default: 3333)")

	dir = flag.String("p", ".", "directory with files to serve")

	flag.Parse()

	if *logLevel {
		slog.SetLogLoggerLevel(slog.LevelDebug)
		slog.Debug("Set logging level to debug")
	}

	return
}

func main() {
	port , dir := parseArgs()
	server.RunServer(port, dir)
}
