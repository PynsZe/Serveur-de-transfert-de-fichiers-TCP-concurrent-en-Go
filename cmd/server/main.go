package main

import (
	"flag"
	"log/slog"
	"time"

	"gitlab.univ-nantes.fr/iutna.info2.r305/proj/internal/app/server"
)

const AdminPort = "4444" // J'utilise 4444 pour éviter les ports privilégiés (<1024)
func parseArgs() (port *string, dir *string, timeout *int) {

	logLevel := flag.Bool("d", false, "enable debug log level")
	port = flag.String("p", "3333", "server port (default: 3333)")
	dir = flag.String("dir", ".", "directory with files to serve")
	timeout = flag.Int("t", 5, "time limit")
	flag.Parse()

	if *logLevel {
		slog.SetLogLoggerLevel(slog.LevelDebug)
		slog.Debug("Set logging level to debug")
	}

	return
}

func main() {
	port, dir, timeout := parseArgs()
	// Lancement du listener des commandes d'administration sur le port 4444 en arrière-plan
	go server.RunAdminServer(AdminPort, *dir, time.Duration(*timeout)*time.Second)

	// Le serveur principal s'exécute sur le thread principal
	server.RunServer(port, dir, time.Duration(*timeout)*time.Second)
}
