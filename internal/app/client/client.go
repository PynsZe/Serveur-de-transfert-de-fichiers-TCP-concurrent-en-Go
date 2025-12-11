package client

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"

	"gitlab.univ-nantes.fr/iutna.info2.r305/proj/internal/pkg/proto"
)

/**
* La fonction Run permet de lancer le client
*
* @param remote : l'adresse du serveur distant
 */
func Run(remote string, dir *string) {

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
		fmt.Print("> ")

		line, err := stdin.ReadString('\n')
		if err != nil {
			slog.Error(err.Error())
			return
		}

		cmd, value, parsErr := proto.Parse(strings.TrimSpace(line))
		if parsErr != nil {
			slog.Error("Malformed command, error : " + parsErr.Error())
			continue
		}

		switch cmd {
		case "End", "List", "Get", "Cd":
			_, err = out.WriteString(line)
			if err != nil {
				slog.Error(err.Error())
				return
			}
			out.Flush()
		case "Terminate", "Hide", "Reveal":
			
			_, err = out.WriteString(line)
			if err != nil {
				slog.Error(err.Error())
				return
			}
			out.Flush()
		default:
			slog.Warn("Unknown command: " + cmd)
			continue
		}

		switch cmd {
        case "End":
            slog.Info("Session closed by user")
            return

        case "List":
            slog.Debug("Sending command: " + cmd + " " + value)
            handleListClient(in, out)

        case "Get":
            slog.Debug("Sending command: " + cmd + " " + value)
            handleGetClient(in, out, *dir, value)

		case "Cd": // <--- NOUVEAU CAS POUR GÉRER LA RÉPONSE DE Cd
			// Lit la réponse simple envoyée par le serveur (OK ou erreur)
			response, err := in.ReadString('\n')
			if err != nil {
				slog.Error("Erreur lors de la réception de la réponse Cd : " + err.Error())
				return
			}
			response = strings.TrimSpace(response)
			
			if response == "OK" {
				fmt.Println("Répertoire courant mis à jour.")
			} else {
				fmt.Printf("Échec du changement de répertoire: %s\n", response)
			}

        // NOUVEAU BLOC : Gère la réponse des commandes admin
        case "Terminate", "Hide", "Reveal":
            // CORRECTION : On lit la réponse ici, APRÈS l'envoi.
            response, err := in.ReadString('\n')
            if err != nil {
                // Si la lecture échoue, la connexion est probablement coupée par le serveur.
                if cmd == "Terminate" {
                    fmt.Println("Commande Terminate exécutée. Déconnexion forcée du client.")
                } else {
                    slog.Error(fmt.Sprintf("Connexion interrompue après %s: %s", cmd, err.Error()))
                }
                return // Quitter la session
            }

            response = strings.TrimSpace(response)
            fmt.Printf("Serveur réponse (%s): %s\n", cmd, response)

            // Si Terminate est un succès, on quitte le client
            if cmd == "Terminate" && response == "OK" {
                slog.Info("Serveur arrêté par Terminate. Déconnexion.")
                return 
            }
        }
	}
}

/*
* La fonction handleListClient permet de gérer la commande List côté client
*
* @param in : le reader pour lire les données du serveur
* @param out : le writer pour envoyer des données au serveur
 */
func handleListClient(in *bufio.Reader, out *bufio.Writer) {
	// Lecture de "FileCnt X"
	header, err := in.ReadString('\n')
	if err != nil {
		slog.Error("Erreur réception FileCnt : " + err.Error())
		return
	}
	header = strings.TrimSpace(header)
	fmt.Println(header)

	// Extraction du nombre de fichiers
	parts := strings.Fields(header)
	if len(parts) != 2 {
		slog.Error("Réponse FileCnt incorrecte")
		return
	}

	// Conversion X en entier
	var count int
	fmt.Sscanf(parts[1], "%d", &count)

	slog.Debug("LECTURE DES FICHIER ENVOYEES PAR LE SERVEUR")
	// Lecture des X lignes suivantes
	for i := 0; i < count; i++ {
		line, err := in.ReadString('\n')
		if err != nil {
			slog.Error("Erreur lecture fichier : " + err.Error())
			return
		}
		fmt.Print(line)
	}

	// Une fois la liste reçue → envoyer OK
	slog.Debug("→ Envoi OK")
	_, err = out.WriteString("OK\n")
	if err != nil {
		slog.Error("Erreur envoi OK : " + err.Error())
		return
	}
	out.Flush()
}

func handleGetClient(in *bufio.Reader, out *bufio.Writer, pathDir string, file string) {
	//commence la recuperation du fichier.
	slog.Debug("Demande de téléchargement du fichier : " + file)

	// Lecture de la réponse du serveur
	// Header : Start
	header, err := in.ReadString('\n')
	slog.Debug("header : " + header)
	if err != nil {
		slog.Error("Erreur réception FileSize : " + err.Error())
		return
	}
	header = strings.TrimSpace(header)
	fmt.Println(header)
	slog.Debug("Réponse serveur : " + header)

	if header != "Start" {
        slog.Warn("Transfert annulé par le serveur: " + header)
        // La transaction est terminée. On retourne au prompt principal.
        return 
    }

	// Lecture de la taille du fichier
	fileSize, err := in.ReadString('\n')
	slog.Debug("fileSize : " + fileSize)
	if err != nil {
		slog.Error("Erreur réception FileSize : " + err.Error())
		return
	}
	fileSize = strings.TrimSpace(fileSize)
	fmt.Println("→ Taille du fichier : " + fileSize)

	var count int
	fmt.Sscanf(fileSize, "%d", &count)

	// Ouvrir le fichier en écriture
	f, err := os.Create(pathDir + "/" + file)
	if err != nil {
		slog.Error("Erreur création fichier : " + err.Error())
		return
	}
	defer f.Close()

	// Lire les données du fichier
	receivedBytes := 0
	buffer := make([]byte, 1024)
	for receivedBytes < count {
		n, err := in.Read(buffer)
		if err != nil {
			slog.Error("Erreur lecture données fichier : " + err.Error())
			return
		}
		f.Write(buffer[:n])
		receivedBytes += n
	}

	fmt.Printf("Fichier %s téléchargé (%d octets)\n", file, count)

	// Une fois la liste reçue → envoyer OK
	fmt.Println("→ Envoi OK")
	_, err = out.WriteString("OK\n")
	if err != nil {
		slog.Error("Erreur envoi OK : " + err.Error())
		return
	}
	out.Flush()
}
