package server

import (
	"bufio"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"time"
	// Les imports sync (Mutex, WaitGroup, Channel) sont dans le fichier `server.go` principal.
)

/* ====================================================
                  GESTION DU CLIENT ADMINISTRATEUR
==================================================== */

/**
* La fonction handleAdminClient gère la communication avec un client connecté
* sur le port d'administration.
*
* Cette goroutine gère la session d'un unique client administrateur.
*
* @param c : la connexion réseau avec le client administrateur
* @param rootDir : le répertoire racine des fichiers à servir
 */
func handleAdminClient(c net.Conn, rootDir string) {

	// Note sur clientCount : Le compteur global de clients (clientCount) n'est pas incrémenté ici
	// pour que `displayClientCount` n'affiche que le nombre de connexions "utilisateurs" classiques.

	currentDir := rootDir // Répertoire courant de l'administrateur

	// 1. Ajout à la liste des connexions actives (pour être surveillé par Terminate)
	activeConnectionsMux.Lock()
	activeConnections[c] = true
	activeConnectionsMux.Unlock()

	slog.Info("Incoming ADMIN connection from " + c.RemoteAddr().String())
	// 2. Logique de nettoyage à la fin de la goroutine (defer)
	defer func() {
		// **SYNCHRONISATION (Mutex)** : Déverrouille le statut admin (permet une nouvelle connexion admin)
		adminActiveMux.Lock()
		adminActive = false
		adminActiveMux.Unlock()

		// Retire de la liste des connexions actives
		activeConnectionsMux.Lock()
		delete(activeConnections, c)
		activeConnectionsMux.Unlock()

		c.Close()
		slog.Info("ADMIN Connexion closed for" + c.RemoteAddr().String())
		// **SYNCHRONISATION (WaitGroup)** : Indique que cette goroutine est terminée
		clientWG.Done()
	}()

	reader := bufio.NewReader(c)
	writer := bufio.NewWriter(c)

	// 3. Boucle de lecture des commandes
	for {
		commandLine, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				slog.Error("N'a pas pu lire la commande ADMIN" + err.Error())
			}
			break // Sort de la boucle, déclenchant le `defer`
		}

		commandLine = strings.TrimSpace(commandLine)
		slog.Debug("Commande ADMIN reçu :" + commandLine)

		partie := strings.Fields(commandLine)
		if len(partie) == 0 {
			slog.Info("C'est vide")
			continue
		}
		commande := partie[0]

		// --- COMMANDES D'ADMINISTRATION et partagées ---

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
			// L'exécution de Terminate arrête l'intégralité du processus serveur (os.Exit)
			handleTerminate(c, writer)
			return // Termine immédiatement la goroutine
		} else if commande == "List" {
			// L'admin peut aussi demander la liste des fichiers
			handleList(c, writer, reader, currentDir)
		} else if commande == "Cd" {
			// L'admin peut aussi changer de répertoire courant
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

/* ====================================================
                BOUCLE PRINCIPALE DU SERVEUR ADMIN
==================================================== */

/**
* La fonction RunAdminServer lance le listener spécifique pour les connexions
* d'administration (Terminate, Hide, Reveal).
*
* @param port : le port d'écoute dédié à l'administration
* @param rootDir : le répertoire racine des fichiers à servir
* @param duration : la durée maximale d'attente (non utilisée ici)
 */
func RunAdminServer(port string, rootDir string, duration time.Duration) {

	cleanedRootDir := filepath.Clean(rootDir)

	// 1. Démarrage de l'écoute TCP
	l, e := net.Listen("tcp", ":"+port)
	if e != nil {
		slog.Error("Erreur lors de l'écoute du port d'administration", "port", port, "erreur", e.Error())
		return
	}
	defer l.Close()
	slog.Debug("Now listening for ADMIN commands on port " + port)

	// 2. Boucle d'acceptation des connexions
	for {
		// **SYNCHRONISATION (Channel)** : Écoute le canal de terminaison
		select {
		case <-terminateChan:
			// Signal reçu, fermer l'écoute et sortir
			l.Close()
			return
		default:
			// Accepte une nouvelle connexion
			c, e := l.Accept()
			if e != nil {
				// Gère l'erreur si l'écoute est fermée
				if strings.Contains(e.Error(), "use of closed network connection") {
					return
				}
				slog.Error("Erreur, ne peut pas accepté connexion admin: " + e.Error())
				continue
			}

			// 3. Gestion de la connexion unique de l'admin
			// **SYNCHRONISATION (Mutex)** : Protège l'accès à `adminActive`
			adminActiveMux.Lock()
			if adminActive {
				// Si un admin est déjà connecté, on refuse la nouvelle connexion
				adminActiveMux.Unlock()
				slog.Warn("Tentative de connexion ADMIN multiple. Refus de " + c.RemoteAddr().String())

				c.Write([]byte("AdminConnectionRefused\n"))
				c.Close()
				continue
			}

			// Enregistre le nouvel admin actif
			adminActive = true
			adminActiveMux.Unlock()

			// 4. Lancement de la goroutine de gestion
			// **SYNCHRONISATION (WaitGroup)** : Incrémente pour la nouvelle goroutine admin
			clientWG.Add(1)
			go handleAdminClient(c, cleanedRootDir)
		}
	}
}
