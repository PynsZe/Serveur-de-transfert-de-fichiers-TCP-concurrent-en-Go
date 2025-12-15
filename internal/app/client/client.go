package client

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"gitlab.univ-nantes.fr/iutna.info2.r305/proj/internal/pkg/proto"
)

// NOTE : Ce code n'utilise pas de 'channels' ni de 'mutex'.
//
// * **Channels (canaux)** : Sont généralement utilisés en Go pour la communication
//   entre goroutines (processus légers concurrents). Le client ici est synchrone,
//   traitant une commande à la fois dans une seule goroutine principale.
//
// * **Mutex (verrous de exclusion mutuelle)** : Sont utilisés pour protéger des
//   ressources partagées (variables, structures de données) contre les accès
//   concurrents (race conditions) par plusieurs goroutines. Étant donné qu'il n'y a
//   qu'une seule goroutine gérant la connexion et l'I/O ici, un mutex n'est pas nécessaire.

/**
* La fonction Run permet de lancer le client.
*
* Elle établit la connexion TCP, gère la boucle principale d'interaction
* avec l'utilisateur et communique avec le serveur.
*
* @param remote : l'adresse du serveur distant (ex: "localhost:8080")
* @param dir : le répertoire local où les fichiers téléchargés seront sauvegardés
* @param duration : (non utilisé dans la logique actuelle, peut être pour un timeout ou une reconnexion)
 */
func Run(remote string, dir *string, duration time.Duration) {

	// 1. Établissement de la connexion TCP
	c, e := net.Dial("tcp", remote)
	if e != nil {
		slog.Error(e.Error()) // Journaliser l'erreur de connexion
		return
	}

	// 2. Fermeture différée de la connexion
	// 'defer' garantit que la fonction anonyme (fermeture de la connexion) sera appelée
	// juste avant que la fonction Run ne se termine (que ce soit par un 'return' normal
	// ou une panique).
	defer func() {
		c.Close()
		slog.Debug("Connection closed")
	}()

	slog.Info("Connected to " + c.RemoteAddr().String()) // Afficher l'adresse distante

	// 3. Initialisation des lecteurs/écrivains pour l'I/O
	// 'in' lit les données du serveur (connexion TCP)
	in := bufio.NewReader(c)
	// 'out' écrit les données vers le serveur (connexion TCP)
	out := bufio.NewWriter(c)
	// 'stdin' lit l'entrée utilisateur depuis la console
	stdin := bufio.NewReader(os.Stdin)

	// Boucle principale d'interaction avec l'utilisateur
	for {

		fmt.Print("> ") // Affiche le prompt

		// 4. Lecture de la commande utilisateur
		line, err := stdin.ReadString('\n')
		if err != nil {
			slog.Error(err.Error()) // Erreur de lecture stdin
			return
		}

		// 5. Analyse de la commande
		// Parse la ligne lue pour extraire la commande (cmd) et sa valeur/argument (value).
		// La fonction proto.Parse est supposée gérer le format des commandes.
		cmd, value, parsErr := proto.Parse(strings.TrimSpace(line))
		if parsErr != nil {
			slog.Error("Malformed command, error : " + parsErr.Error())
			continue // Retourne au début de la boucle pour une nouvelle commande
		}

		// 6. Envoi de la commande au serveur (Phase 1 : Envoi)
		// Seules certaines commandes sont envoyées directement ici.
		switch cmd {
		case "End", "List", "Get", "Cd":
			_, err = out.WriteString(line) // Écrit la ligne entière (commande + arguments)
			if err != nil {
				slog.Error(err.Error())
				return
			}
			out.Flush() // Force l'envoi immédiat des données du buffer 'out' vers le réseau
		case "Terminate", "Hide", "Reveal": // Commandes d'administration
			_, err = out.WriteString(line)
			if err != nil {
				slog.Error(err.Error())
				return
			}
			out.Flush()
		default:
			slog.Warn("Unknown command: " + cmd) // Commande non reconnue localement
			continue
		}

		// 7. Traitement de la réponse du serveur (Phase 2 : Réponse)
		switch cmd {
		case "End":
			// La commande 'End' est gérée localement, le client se termine après l'envoi.
			slog.Info("Session closed by user")
			return

		case "List":
			// Délègue la gestion du protocole de la commande List à une fonction dédiée.
			slog.Debug("Sending command: " + cmd + " " + value)
			handleListClient(in, out)

		case "Get":
			// Délègue la gestion du protocole de la commande Get à une fonction dédiée.
			slog.Debug("Sending command: " + cmd + " " + value)
			handleGetClient(in, out, *dir, value)

		case "Cd":
			// Lit la réponse simple envoyée par le serveur (doit être "OK" ou un message d'erreur).
			response, err := in.ReadString('\n')
			if err != nil {
				slog.Error("Erreur lors de la réception de la réponse Cd : " + err.Error())
				return
			}
			response = strings.TrimSpace(response) // Nettoie les espaces et le '\n'

			// Affiche le résultat à l'utilisateur
			if response == "OK" {
				fmt.Println("Répertoire courant mis à jour.")
			} else {
				fmt.Printf("Échec du changement de répertoire: %s\n", response)
			}

		// Gestion des commandes d'administration (Terminate, Hide, Reveal)
		case "Terminate", "Hide", "Reveal":
			// Lit la réponse simple du serveur à ces commandes (doit être "OK" ou un message d'erreur).
			response, err := in.ReadString('\n')
			if err != nil {
				// Si Terminate a été envoyé, l'erreur pourrait signifier que le serveur
				// a fermé la connexion après s'être arrêté.
				if cmd == "Terminate" {
					fmt.Println("Commande Terminate exécutée. Déconnexion forcée du client.")
				} else {
					slog.Error(fmt.Sprintf("Connexion interrompue après %s: %s", cmd, err.Error()))
				}
				return // Quitter la session
			}

			response = strings.TrimSpace(response)
			fmt.Printf("Serveur réponse (%s): %s\n", cmd, response)

			// Si Terminate réussit, le client doit se déconnecter
			if cmd == "Terminate" && response == "OK" {
				slog.Info("Serveur arrêté par Terminate. Déconnexion.")
				return // Quitter la boucle et déclencher le 'defer c.Close()'
			}
		}
	}
}

/**
* La fonction handleListClient permet de gérer le protocole de la commande List côté client.
*
* Elle attend que le serveur envoie un en-tête de compte de fichiers ("FileCnt X"),
* lit les X lignes suivantes (les noms de fichiers), puis envoie un "OK"
* pour signaler la fin de la réception.
*
* @param in : le reader pour lire les données du serveur
* @param out : le writer pour envoyer des données au serveur
 */
func handleListClient(in *bufio.Reader, out *bufio.Writer) {
	// 1. Lecture de l'en-tête "FileCnt X"
	header, err := in.ReadString('\n')
	if err != nil {
		// Vérifie si l'erreur est due à une fermeture de connexion par le serveur
		isDown(err)
		slog.Error("Erreur réception FileCnt : " + err.Error())
		return
	}
	header = strings.TrimSpace(header)
	fmt.Println(header)

	// 2. Extraction du nombre de fichiers (X)
	parts := strings.Fields(header)
	if len(parts) != 2 {
		slog.Error("Réponse FileCnt incorrecte")
		return
	}

	var count int
	// Utilisation de Sscanf pour convertir la chaîne du compte en entier
	fmt.Sscanf(parts[1], "%d", &count)

	slog.Debug("LECTURE DES FICHIER ENVOYEES PAR LE SERVEUR")
	// 3. Lecture des X lignes suivantes (les entrées du répertoire)
	for i := 0; i < count; i++ {
		line, err := in.ReadString('\n')
		if err != nil {
			isDown(err)
			slog.Error("Erreur lecture fichier : " + err.Error())
			return
		}
		fmt.Print(line) // Affiche l'entrée lue
	}

	// 4. Envoi de l'accusé de réception "OK"
	// Une fois la liste reçue, on signale au serveur que le transfert est complet.
	slog.Debug("→ Envoi OK")
	_, err = out.WriteString("OK\n")
	if err != nil {
		slog.Error("Erreur envoi OK : " + err.Error())
		return
	}
	out.Flush() // Envoi immédiat
}

/**
* La fonction handleGetClient permet de gérer le protocole de la commande Get côté client.
*
* Elle crée le répertoire local cible, attend la confirmation de début du transfert
* ("Start"), reçoit la taille du fichier, télécharge les données binaires et
* enregistre le fichier localement, puis envoie un "OK".
*
* @param in : le reader pour lire les données du serveur
* @param out : le writer pour envoyer des données au serveur
* @param pathDir : le répertoire local de destination
* @param file : le nom du fichier à télécharger
 */
func handleGetClient(in *bufio.Reader, out *bufio.Writer, pathDir string, file string) {
	// 1. Création du répertoire de destination si nécessaire
	err := os.MkdirAll(pathDir, os.ModePerm)
	if err != nil {
		slog.Error("Erreur lors de la création du répertoire : " + err.Error())
		return
	}
	slog.Debug("Demande de téléchargement du fichier : " + file)

	// 2. Lecture de la réponse initiale du serveur
	// Le serveur doit envoyer "Start" pour commencer ou un message d'erreur.
	header, err := in.ReadString('\n')
	slog.Debug("header : " + header)
	if err != nil {
		isDown(err)
		slog.Error("Erreur réception FileSize : " + err.Error())
		return
	}
	header = strings.TrimSpace(header)
	fmt.Println(header)
	slog.Debug("Réponse serveur : " + header)

	// Si la réponse n'est pas "Start", le transfert est annulé (ex: fichier non trouvé).
	if header != "Start" {
		slog.Warn("Transfert annulé par le serveur: " + header)
		return // Retourne au prompt principal
	}

	// 3. Lecture de la taille du fichier
	fileSize, err := in.ReadString('\n')
	slog.Debug("fileSize : " + fileSize)
	if err != nil {
		isDown(err)
		slog.Error("Erreur réception FileSize : " + err.Error())
		return
	}
	fileSize = strings.TrimSpace(fileSize)
	fmt.Println("→ Taille du fichier : " + fileSize)

	var count int
	// Conversion de la taille en octets en entier
	fmt.Sscanf(fileSize, "%d", &count)

	// 4. Création du fichier local en écriture
	f, err := os.Create(pathDir + "/" + file)
	if err != nil {
		slog.Error("Erreur création fichier : " + err.Error())
		return
	}
	// Ferme le fichier une fois que handleGetClient se termine
	defer f.Close()

	// 5. Lecture et écriture des données du fichier
	receivedBytes := 0
	buffer := make([]byte, 8) // Buffer de petite taille pour la lecture
	// Boucle pour lire les données jusqu'à ce que tous les octets soient reçus
	for receivedBytes < count {
		// Lit au maximum la taille du buffer (8 octets)
		n, err := in.Read(buffer)
		if err != nil {
			isDown(err)
			slog.Error("Erreur lecture données fichier : " + err.Error())
			return
		}
		// Écrit les octets lus dans le fichier local
		f.Write(buffer[:n])
		receivedBytes += n
	}

	fmt.Printf("Fichier %s téléchargé (%d octets)\n", file, count)

	// 6. Envoi de l'accusé de réception "OK"
	// Confirme au serveur que le fichier a été entièrement reçu.
	fmt.Println("→ Envoi OK")
	_, err = out.WriteString("OK\n")
	if err != nil {
		slog.Error("Erreur envoi OK : " + err.Error())
		return
	}
	out.Flush()
}

/**
* La fonction isDown vérifie si l'erreur est due à une fin de fichier (EOF)
* indiquant que le serveur a fermé la connexion.
* Si c'est le cas, elle affiche un message et termine le programme.
*
* @param err : l'erreur à vérifier
 */
func isDown(err error) {
	if err == io.EOF {
		fmt.Println("Server closed connection")
		os.Exit(0) // Termine le processus client
	}
}
