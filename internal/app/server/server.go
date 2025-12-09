package server

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

/* etat global du serveur */

var (
	clientCount      int
	terminateRequest bool            // requete pour terminate
	clientCountMux   sync.Mutex      // Mutex pour protéger clientCount
	hiddenFiles      map[string]bool // map[filename] -> true (caché)
	hiddenFilesMux   sync.Mutex      // Mutex pour protéger hiddenFiles
	terminateChan    chan struct{}   // Canal utilisé pour signaler l'arrêt à RunServer
	serverWG         sync.WaitGroup  // Pour attendre la fin de RunServer
	clientWG         sync.WaitGroup  // Pour attendre que tous les handleClient se terminent

	isCommandeUsed    bool       //etats des commandes
	isCommandeUsedMux sync.Mutex // Mutex pour protéger isCommandeUsedount

)

/* compteur pour le nombre de client connecté */
const CountClient = 1 * time.Second

/* compteur en temps reel*/
func displayClientCount() {
	for {

		clientCountMux.Lock()
		currentCount := clientCount
		clientCountMux.Unlock()

		// Afficher le compte actuel
		slog.Info(fmt.Sprintf("Clients connectés: %d", currentCount))

		// Attendre
		time.Sleep(CountClient)
	}
}

func setIsCommandeUsed(val bool) {
	isCommandeUsedMux.Lock()
	isCommandeUsed = val
	isCommandeUsedMux.Unlock()
}

func getIsCommandeUsed() bool {
	isCommandeUsedMux.Lock()
	val := isCommandeUsed
	isCommandeUsedMux.Unlock()
	return val
}

func RunServer(port *string, dir *string) {

	if hiddenFiles == nil {
		hiddenFiles = make(map[string]bool)
	}

	terminateChan = make(chan struct{})
	serverWG.Add(1) // Le serveur principal doit être attendu

	l, e := net.Listen("tcp", ":"+*port)
	if e != nil {
		slog.Error(e.Error())
		return
	}
	defer func() {
		l.Close()
		serverWG.Done() // Indiquer que RunServer est terminé
		slog.Debug("Stopped listening on port " + *port)
	}()
	slog.Debug("Now listening on port " + *port)
	slog.Info("Files coming from directory " + *dir)

	/* compteur mis en route */
	go displayClientCount()
	go checkForTerminate(l)

	for {
		select {
		case <-terminateChan:
			// Signal reçu, arrêter d'accepter de nouvelles connexions
			slog.Info("Signal de TERMINATE reçu. Arrêt de l'écoute.")
			return
		default:
			// Si pas de signal, continue à accepter
			c, e := l.Accept()
			if e != nil {
				// Si l'erreur est due à la fermeture de l'écoute, on sort
				if strings.Contains(e.Error(), "erreur est due à la fermeture de l'écoute") {
					return
				}
				slog.Error("Erreur, ne peut pas accepté" + e.Error())
				continue
			}
			/* lancement d'une nouvelle go routine pour le client */
			clientWG.Add(1) //  Incrémenter le WaitGroup avant de lancer la goroutine
			go handleClient(c, *dir)
		}
	}
}

func checkForTerminate(l net.Listener) {
	for {
		if terminateRequest {
			isCommandeUsedMux.Lock()
			commandInProgress := isCommandeUsed
			isCommandeUsedMux.Unlock()

			if !commandInProgress {
				slog.Info("Aucune commande utilisée : on ferme le serveur")
				l.Close()
				return
			}
		}
		time.Sleep(100 * time.Millisecond) // Vérifier toutes les 100ms
	}
}

func RunAdminServer(port string, rootDir string) {

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
			// Les commandes Admin sont des clients comme les autres
			clientWG.Add(1)
			go handleAdminClient(c, rootDir)
		}
	}
}

/* fonction pour gérer les commandes  des tout les clients : list get et end  */
// Ajouter cette nouvelle fonction dans server/server.go

func handleAdminClient(c net.Conn, rootDir string) {

	// Le comptage des clients (clientCount) reste dans handleClient pour les connexions classiques.
	// Pour l'admin, on utilise seulement le WaitGroup.

	slog.Info("Incoming ADMIN connection from " + c.RemoteAddr().String())
	defer func() {
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
			handleHide(writer, reader, rootDir, filename)
		} else if commande == "Reveal" {
			if len(partie) < 2 {
				slog.Warn("commande incomplete: Reveal")
				writer.WriteString("CommandIncomplete\n")
				writer.Flush()
				continue
			}
			filename := partie[1]
			handleReveal(writer, reader, rootDir, filename)
		} else if commande == "Terminate" {
			// C'est le seul endroit où Terminate devrait être appelée
			handleTerminate(writer, reader)
			return
		} else {
			slog.Warn("command ADMIN inconnue " + commandLine)
			writer.WriteString("UnknownCommand\n")
			writer.Flush()
		}
	}
}

func handleClient(c net.Conn, rootDir string) {

	clientCountMux.Lock()
	clientCount++
	clientCountMux.Unlock()

	slog.Info("Incoming connection from " + c.RemoteAddr().String())
	defer func() {
		c.Close()
		slog.Info("Connexion closed for" + c.RemoteAddr().String())

		clientCountMux.Lock()
		clientCount--
		clientCountMux.Unlock()

		clientWG.Done() // Indiquer que cette goroutine est terminée
	}()

	/* boucle pour list get et end */

	/* time.Sleep(10*time.Second) */

	reader := bufio.NewReader(c)
	writer := bufio.NewWriter(c)

	for {
		commandLine, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				slog.Error("N'a pas pu lire la commande" + err.Error())
			}
			break
		}

		commandLine = strings.TrimSpace(commandLine)
		slog.Debug("Commande reçu :" + commandLine)

		/* separe la commande du nom du fichier */

		partie := strings.Fields(commandLine)
		if len(partie) == 0 {
			slog.Info("C'est vide")
			continue
		}
		commande := partie[0]

		/* commandes */

		if commande == "List" {
			handleList(writer, reader, rootDir)
		} else if commande == "Get" {
			if len(partie) < 2 {
				slog.Warn("commande incomplete")
				continue
			}
			filename := partie[1]
			handleGet(writer, reader, rootDir, filename)
		} else if commande == "End" { // Seule commande de déconnexion client
			slog.Info("Client" + c.RemoteAddr().String() + "veut se deconnecter")
			break
			// Supprimer les `else if` pour Hide, Reveal, et Terminate ici
		} else {
			slog.Warn("command inconnue " + commandLine)
			writer.WriteString("UnknownCommand\n")
			writer.Flush()
		}
	}
}

func handleList(w *bufio.Writer, r *bufio.Reader, pathDir string) {
	setIsCommandeUsed(true)
	/* lecture  du contenue du dossier */

	entre, err := os.ReadDir(pathDir)
	if err != nil {
		slog.Error("N'A PAS PU LIRE LE DOSSIER" + err.Error())
		w.WriteString("FileCnt 0\n") /*  Envoi d'une liste vide pour ne pas bloquer le client. */
		w.Flush()
		return
	}

	hiddenFilesMux.Lock()
	defer hiddenFilesMux.Unlock()

	/* preparation du message pour filecnt + comptage des fichiers  */
	/* preparation d'une liste de messages de fichiers a envoyer */

	messages := []string{}

	for _, entree := range entre {
		if _, isHidden := hiddenFiles[entree.Name()]; isHidden {
			continue
		}
		if entree.Type().IsRegular() {
			info, _ := entree.Info()
			ligneMess := fmt.Sprintf("%s %d\n", entree.Name(), info.Size())
			messages = append(messages, ligneMess)
		}
	}

	count := len(messages)

	/* envoyer compteur filecnt x */

	countMsg := fmt.Sprintf("FileCnt %d\n", count)
	w.WriteString(countMsg)
	slog.Debug("Serveur envoi: " + strings.TrimSpace(countMsg))

	/* envoyer la liste detaillé */

	for _, msg := range messages {
		w.WriteString(msg)
		slog.Debug("Serveur envoi :" + strings.TrimSpace(msg))
	}

	w.Flush()
	slog.Debug(fmt.Sprintf("%d fichiers listés et envoyés", count))

	ok, err := r.ReadString('\n')
	if err != nil || strings.TrimSpace(ok) != "OK" {
		slog.Error("LE CLIENT N'A PAS REPONDUS OK")
		if err == io.EOF {
			return
		}
		return
	}
	slog.Debug("Le client a validé la reception avec ok")
	setIsCommandeUsed(false)
}

func handleGet(w *bufio.Writer, r *bufio.Reader, pathDir string, filename string) {
	setIsCommandeUsed(true)

	// TODO () : gérer les erreurs et les fichiers cachés
	// TODO () : gérer les erreurs de path pas existants
	// TODO () : gérer l'enregistrement des fichiers dans le repo voulu

	filePath := filepath.Join(pathDir, filename)

	hiddenFilesMux.Lock()
	_, isHidden := hiddenFiles[filename]
	hiddenFilesMux.Unlock()

	if isHidden {
		slog.Warn("Tentative de GET sur un fichier caché: " + filename)
		w.WriteString("FileHidden\n")
		w.Flush()
		return
	}

	/* ouverture du fichier */
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Warn("Fichier inconnue" + filename)
			w.WriteString("FileUnknown\n")
		} else {
			slog.Error("Erreur lors de l'ouverture du fichier" + err.Error())
			w.WriteString("ServerErreur\n")
		}
		w.Flush()
		return
	}

	defer file.Close()

	/* recuperer les infos pour le log  */
	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()

	/* envoie signal debut et données binaire  */
	slog.Debug(fmt.Sprintf("Transfert de %s (%d octets)", filename, fileSize))

	/* envoie du start */
	w.WriteString("Start\n")
	w.Flush()

	w.WriteString(fmt.Sprintf("%d\n", fileSize))
	w.Flush()

	/* copie du fichier vers le reseau */
	n, err := io.Copy(w, file)
	w.Flush()

	if err != nil || n != fileSize {
		slog.Error(
			"Transfert incomplet ou échoué",
			"fichier", filename,
			"envoyé", n,
			"attendu", fileSize,
			"erreur", err)
		return
	}
	slog.Debug(fmt.Sprintf("Transfert terminé %d octets envoyés", n))

	/* attente du ok par le client */

	ok, err := r.ReadString('\n')
	if err != nil || strings.TrimSpace(ok) != "OK" {
		slog.Error("LE CLIENT N'A PAS REPONDUS OK")
		return
	}

	slog.Info("Fichier téléchargé",
		"nom", filename,
		"taille", n,
		"client", file.Name())

	slog.Debug("Le client a validé la reception avec ok")
	setIsCommandeUsed(false)
}

func handleHide(w *bufio.Writer, r *bufio.Reader, pathDir string, filename string) {

	filePath := filepath.Join(pathDir, filename)
	// Vérifie l'existence et le type du fichier
	fileInfo, err := os.Stat(filePath)

	if os.IsNotExist(err) || err != nil || !fileInfo.Mode().IsRegular() {
		slog.Warn("Tentative de cacher un fichier inconnu ou non régulier: " + filename)
		w.WriteString("FileUnknown\n")
		w.Flush()
		return
	}

	//Ajoute le fichier a la liste des fichiers cachés
	hiddenFilesMux.Lock()
	defer hiddenFilesMux.Unlock()

	if _, alreadyHidden := hiddenFiles[filename]; !alreadyHidden {
		hiddenFiles[filename] = true
		slog.Info("Fichier caché: " + filename)
	}

	// Répondre OK
	w.WriteString("OK\n")
	w.Flush()
}

func handleReveal(w *bufio.Writer, r *bufio.Reader, pathDir string, filename string) {

	filePath := filepath.Join(pathDir, filename)

	//Vérifie l'existence sur le disque
	fileInfo, err := os.Stat(filePath)

	if os.IsNotExist(err) || err != nil || !fileInfo.Mode().IsRegular() {
		slog.Warn("Tentative de révéler un fichier inconnu ou non régulier: " + filename)
		w.WriteString("FileUnknown\n")
		w.Flush()
		return
	}

	// Retire le fichier de la liste des fichiers cachés
	hiddenFilesMux.Lock()
	defer hiddenFilesMux.Unlock()

	if _, wasHidden := hiddenFiles[filename]; wasHidden {
		delete(hiddenFiles, filename) // Supprime l'entrée de la map
		slog.Info("Fichier révélé: " + filename)
	} else {
		slog.Debug("Le fichier " + filename + " n'était pas marqué comme caché.")
	}

	//Répond OK
	w.WriteString("OK\n")
	w.Flush()
}

func handleTerminate(w *bufio.Writer, r *bufio.Reader) {

	slog.Warn("COMMANDE TERMINATE REÇUE. Début de l'arrêt ordonné.")

	// Vérifier le nombre de clients connectés
	/* 	clientCountMux.Lock()
	   	currentCount := clientCount
	   	clientCountMux.Unlock() */

	terminateRequest = true
	/*
	 if currentCount > 0 {
	 	slog.Warn(fmt.Sprintf("Impossible de terminer : %d clients sont encore connectés.", currentCount))
	 	w.WriteString("TerminateRefused\n")
	 	w.Flush()
	 	return
	 } */

	slog.Warn("COMMANDE TERMINATE REÇUE. Début de l'arrêt ordonné.")

	// Stopper l'écoute de RunServer
	close(terminateChan)
	slog.Info("Arrêt de l'écoute des nouvelles connexions.")

	// Attend que tous les clients actifs terminent leurs commandes et se déconnectent.
	clientWG.Wait()
	slog.Info("Tous les clients sont déconnectés. Serveur arrêté.")

	//Envoie la confirmation au client de contrôle
	w.WriteString("OK\n")
	w.Flush()

	// Arrêt du processus
	/* serverWG.Wait() */
	slog.Info("Processus serveur terminé.")
	os.Exit(0) // Arrêter le processus Go
}
