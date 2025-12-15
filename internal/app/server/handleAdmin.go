package server

import (
	"bufio"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"time"
)

func handleAdminClient(c net.Conn, rootDir string) {

	// Le comptage des clients (clientCount) reste dans handleClient pour les connexions classiques.
	// Pour l'admin, on utilise seulement le WaitGroup.

	currentDir := rootDir

	activeConnectionsMux.Lock()
	activeConnections[c] = true
	activeConnectionsMux.Unlock()

	slog.Info("Incoming ADMIN connection from " + c.RemoteAddr().String())
	defer func() {

		adminActiveMux.Lock()
		adminActive = false
		adminActiveMux.Unlock()

		activeConnectionsMux.Lock()
		delete(activeConnections, c)
		activeConnectionsMux.Unlock()
		c.Close()
		slog.Info("ADMIN Connexion closed for" + c.RemoteAddr().String())
		clientWG.Done() // Indiquer que cette goroutine est terminée
	}()

	reader := bufio.NewReader(c)
	writer := bufio.NewWriter(c)

	for {
		commandLine, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				slog.Error("N'a pas pu lire la commande ADMIN" + err.Error())
			}
			break
		}

		commandLine = strings.TrimSpace(commandLine)
		slog.Debug("Commande ADMIN reçu :" + commandLine)

		partie := strings.Fields(commandLine)
		if len(partie) == 0 {
			slog.Info("C'est vide")
			continue
		}
		commande := partie[0]

		// --- COMMANDES D'ADMINISTRATION ---

		if commande == "Hide" {
			if len(partie) < 2 {
				slog.Warn("commande incomplete: Hide")
				writer.WriteString("CommandIncomplete\n")
				writer.Flush()
				continue
			}
			filename := partie[1]
			handleHide(writer, reader, currentDir, filename)
		} else if commande == "Reveal" {
			if len(partie) < 2 {
				slog.Warn("commande incomplete: Reveal")
				writer.WriteString("CommandIncomplete\n")
				writer.Flush()
				continue
			}
			filename := partie[1]
			handleReveal(writer, reader, currentDir, filename)
		} else if commande == "Terminate" {
			// C'est le seul endroit où Terminate devrait être appelée
			handleTerminate(c, writer)
			return
		} else if commande == "List" {
			handleList(c, writer, reader, currentDir)
		} else if commande == "Cd" { // <-- NOUVELLE COMMANDE CD
			if len(partie) < 2 {
				slog.Warn("commande incomplete: Cd")
				writer.WriteString("CommandIncomplete\n")
				writer.Flush()
				continue
			}
			targetDir := partie[1]
			// Met à jour le répertoire courant
			newDir := handleChangeDir(c, writer, currentDir, rootDir, targetDir)
			currentDir = newDir
		} else {
			slog.Warn("command ADMIN inconnue " + commandLine)
			writer.WriteString("UnknownCommand\n")
			writer.Flush()
		}
	}
}

func RunAdminServer(port string, rootDir string, duration time.Duration) {

	cleanedRootDir := filepath.Clean(rootDir)

	l, e := net.Listen("tcp", ":"+port)
	if e != nil {
		slog.Error("Erreur lors de l'écoute du port d'administration", "port", port, "erreur", e.Error())
		return
	}
	defer l.Close()
	slog.Debug("Now listening for ADMIN commands on port " + port)

	// Note: Pas besoin de WaitGroup pour ce listener, car il sera arrêté par os.Exit dans handleTerminate

	for {
		select {
		case <-terminateChan:
			// S'assurer que ce listener s'arrête si le signal Terminate est envoyé depuis l'autre listener
			// (Bien que handleTerminate appellera os.Exit, c'est une sécurité)
			l.Close()
			return
		default:
			c, e := l.Accept()
			if e != nil {
				if strings.Contains(e.Error(), "use of closed network connection") {
					return
				}
				slog.Error("Erreur, ne peut pas accepté connexion admin: " + e.Error())
				continue
			}

			adminActiveMux.Lock()
			if adminActive {
				adminActiveMux.Unlock()
				slog.Warn("Tentative de connexion ADMIN multiple. Refus de " + c.RemoteAddr().String())

				c.Write([]byte("AdminConnectionRefused\n"))
				c.Close()
				continue

			}

			adminActive = true
			adminActiveMux.Unlock()

			// Les commandes Admin sont des clients comme les autres
			clientWG.Add(1)
			go handleAdminClient(c, cleanedRootDir)
		}
	}
}
