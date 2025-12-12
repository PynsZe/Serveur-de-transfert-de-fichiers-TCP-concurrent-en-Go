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

	forceClientShutdown chan struct{}

	//Liste des connexions actives
	activeConnections    map[net.Conn]bool
	activeConnectionsMux sync.Mutex

	adminActive    bool       // true si un admin est connecté
	adminActiveMux sync.Mutex // Mutex pour protéger adminActive
)

const (
	DirPrefix     = "├── " // Préfixe pour tous les éléments sauf le dernier
	LastDirPrefix = "└── " // Préfixe pour le dernier élément d'une liste
	ChildPrefix   = "│   " // Préfixe pour l'indentation des enfants (continue)
	EmptyPrefix   = "    " // Préfixe pour l'indentation des enfants (vide)
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

func RunServer(port *string, dir *string, time time.Duration) {

	cleanedRootDir := filepath.Clean(*dir)

	if hiddenFiles == nil {
		hiddenFiles = make(map[string]bool)
	}

	terminateChan = make(chan struct{})
	forceClientShutdown = make(chan struct{})   // Initialisation
	activeConnections = make(map[net.Conn]bool) // Initialisation
	serverWG.Add(1)                             // Le serveur principal doit être attendu

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
	slog.Info("Files coming from directory " + cleanedRootDir) // Utiliser cleanedRootDir

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
			go handleClient(c, cleanedRootDir, time)
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

func RunAdminServer(port string, rootDir string, time time.Duration) {

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
			go handleAdminClient(c, cleanedRootDir, time)
		}
	}
}

/* fonction pour gérer les commandes  des tout les clients : list get et end  */
// Ajouter cette nouvelle fonction dans server/server.go

func handleAdminClient(c net.Conn, rootDir string, time time.Duration) {

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
			handleHide(writer, reader, currentDir, filename, time)
		} else if commande == "Reveal" {
			if len(partie) < 2 {
				slog.Warn("commande incomplete: Reveal")
				writer.WriteString("CommandIncomplete\n")
				writer.Flush()
				continue
			}
			filename := partie[1]
			handleReveal(writer, reader, currentDir, filename, time)
		} else if commande == "Terminate" {
			// C'est le seul endroit où Terminate devrait être appelée
			handleTerminate(writer, reader, time)
			return
		} else if commande == "List" {
			handleList(writer, reader, currentDir, time)
		} else if commande == "Cd" { // <-- NOUVELLE COMMANDE CD
			if len(partie) < 2 {
				slog.Warn("commande incomplete: Cd")
				writer.WriteString("CommandIncomplete\n")
				writer.Flush()
				continue
			}
			targetDir := partie[1]
			// Met à jour le répertoire courant
			newDir := handleChangeDir(writer, currentDir, rootDir, targetDir, time)
			currentDir = newDir
		} else {
			slog.Warn("command ADMIN inconnue " + commandLine)
			writer.WriteString("UnknownCommand\n")
			writer.Flush()
		}
	}
}

func handleClient(c net.Conn, rootDir string, time time.Duration) {

	currentDir := rootDir

	clientCountMux.Lock()
	clientCount++
	clientCountMux.Unlock()

	activeConnectionsMux.Lock()
	activeConnections[c] = true
	activeConnectionsMux.Unlock()

	slog.Info("Incoming connection from " + c.RemoteAddr().String())
	defer func() {
		activeConnectionsMux.Lock()
		delete(activeConnections, c)
		activeConnectionsMux.Unlock()

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
			handleList(writer, reader, currentDir, time)
		} else if commande == "Get" {
			if len(partie) < 2 {
				slog.Warn("commande incomplete")
				continue
			}
			filename := partie[1]
			handleGet(writer, reader, currentDir, filename, time)
		} else if commande == "Cd" { // <-- NOUVELLE COMMANDE CD
			if len(partie) < 2 {
				slog.Warn("commande incomplete: Cd")
				writer.WriteString("CommandIncomplete\n")
				writer.Flush()
				continue
			}
			targetDir := partie[1]
			// Met à jour le répertoire courant
			newDir := handleChangeDir(writer, currentDir, rootDir, targetDir, time)
			currentDir = newDir
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

func handleChangeDir(w *bufio.Writer, currentDir string, rootDir string, targetDir string, timeout time.Duration) string {
	setIsCommandeUsed(true)
	defer setIsCommandeUsed(false)
	start := time.Now()
	// assure que le client ne peut pas utiliser de chemins absolus
	if filepath.IsAbs(targetDir) {
		slog.Warn("Tentative d'utiliser un chemin absolu: " + targetDir)
		w.WriteString("AbsolutePathsForbidden\n")
		w.Flush()
		return currentDir
	}

	// joint le répertoire courant et la cible, puis nettoyer
	absTarget := filepath.Join(currentDir, targetDir)
	cleanTarget := filepath.Clean(absTarget)

	slog.Debug("Tentative de changement de dir vers : " + cleanTarget)

	// assure que le client ne sort jamais du répertoire racine
	/* if !strings.HasPrefix(cleanTarget, rootDir) {
	    slog.Warn("Tentative de sortir du répertoire racine: " + cleanTarget)
	    w.WriteString("AccessDenied\n")
	    w.Flush()
	    return currentDir
	} */

	fileInfo, err := os.Stat(cleanTarget)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Warn("Répertoire cible inconnu: " + cleanTarget)
			w.WriteString("DirectoryUnknown\n")
		} else {
			slog.Error("Erreur Stat sur répertoire: " + err.Error())
			w.WriteString("ServerError\n")
		}
		w.Flush()
		return currentDir
	}

	if !fileInfo.IsDir() {
		slog.Warn("La cible n'est pas un répertoire: " + cleanTarget)
		w.WriteString("NotADirectory\n")
		w.Flush()
		return currentDir
	}
	if time.Since(start) > timeout {
		w.WriteString("Timeout atteint\n")
		w.Flush()
		return currentDir
	}
	slog.Info(fmt.Sprintf("CWD mis à jour de %s à %s", currentDir, cleanTarget))
	w.WriteString("OK\n")
	w.Flush()

	return cleanTarget // Retourne le nouveau chemin nettoyé
}

func buildTree(pathDir string, prefix string, messages *[]string, isRoot bool) error {

	entries, err := os.ReadDir(pathDir)
	if err != nil {
		return err
	}

	dirs := []os.DirEntry{}
	files := []os.DirEntry{}

	// Filtrage et préparation des listes
	hiddenFilesMux.Lock()
	for _, entry := range entries {
		name := entry.Name()

		if strings.HasPrefix(name, ".") {
			continue
		}

		if _, isHidden := hiddenFiles[name]; isHidden {
			continue
		}

		if entry.IsDir() {
			dirs = append(dirs, entry)
		} else if entry.Type().IsRegular() {
			files = append(files, entry)
		}
		// On ignore les autres types (liens symboliques, sockets, etc.)
	}
	hiddenFilesMux.Unlock()

	// L'ordre d'affichage est Répertoires puis Fichiers
	sortedEntries := append(dirs, files...)
	totalEntries := len(sortedEntries)

	if isRoot {
		*messages = append(*messages, ".\n")
	}

	for i, entry := range sortedEntries {
		isLast := i == totalEntries-1

		// Déterminer les préfixes pour l'affichage de la ligne
		linePref := DirPrefix
		nextPref := ChildPrefix
		if isLast {
			linePref = LastDirPrefix
			nextPref = EmptyPrefix
		}

		name := entry.Name()
		var line string

		if entry.IsDir() {
			// Affichage pour le répertoire
			line = fmt.Sprintf("%s%s%s\n", prefix, linePref, name)
		} else {
			// Affichage pour le fichier + Taille
			info, err := entry.Info()
			fileSize := int64(0)
			if err == nil {
				fileSize = info.Size()
			} else {
				slog.Error("Impossible d'obtenir la taille du fichier", "file", name, "error", err.Error())
			}
			line = fmt.Sprintf("%s%s%s %d octets\n", prefix, linePref, name, fileSize)
		}
		*messages = append(*messages, line)

		// Appel récursif pour les sous-répertoires
		if entry.IsDir() {
			newPath := filepath.Join(pathDir, name)
			newPrefix := prefix + nextPref // Ajout de l'indentation pour le niveau suivant

			if err := buildTree(newPath, newPrefix, messages, false); err != nil {
				slog.Error(fmt.Sprintf("Erreur lecture répertoire %s: %s", newPath, err.Error()))
			}
		}
	}
	return nil
}

func handleList(w *bufio.Writer, r *bufio.Reader, pathDir string, timeout time.Duration) {
	setIsCommandeUsed(true)
	defer setIsCommandeUsed(false)
	start := time.Now()
	messages := []string{}

	// Appel à la fonction récursive pour construire l'arborescence
	err := buildTree(pathDir, "", &messages, true) // true pour indiquer la racine (.)
	if err != nil {
		slog.Error("Erreur lors de la construction de l'arborescence: " + err.Error())
		w.WriteString("FileCnt 0\n")
		w.Flush()
		// Tenter de lire OK même si échec (pour vider le buffer si le client envoie OK immédiatement)
		r.ReadString('\n')
		return
	}

	count := len(messages)

	/* envoyer compteur FileCnt X */
	countMsg := fmt.Sprintf("FileCnt %d\n", count)
	w.WriteString(countMsg)
	slog.Debug("Serveur envoi: " + strings.TrimSpace(countMsg))

	/* envoyer la liste détaillée (l'arborescence) */
	for _, msg := range messages {
		w.WriteString(msg)
		// slog.Debug("Serveur envoi :" + strings.TrimSpace(msg)) // Commenté pour ne pas surcharger les logs
	}

	w.Flush()
	slog.Debug(fmt.Sprintf("%d lignes d'arborescence envoyées", count))

	// Attente du OK par le client
	ok, err := r.ReadString('\n')
	if err != nil || strings.TrimSpace(ok) != "OK" {
		slog.Error("LE CLIENT N'A PAS RÉPONDU OK (erreur ou EOF): " + err.Error())
		return
	}
	if time.Since(start) > timeout {
		w.WriteString("Timeout atteint\n")
		w.Flush()
		return
	}
	slog.Debug("Le client a validé la réception avec OK")
}

func handleGet(w *bufio.Writer, r *bufio.Reader, pathDir string, filename string, timeout time.Duration) {
	setIsCommandeUsed(true)
	start := time.Now()
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
	if time.Since(start) > timeout {
		w.WriteString("Timeout atteint\n")
		w.Flush()
		return
	}
	slog.Info("Fichier téléchargé",
		"nom", filename,
		"taille", n,
		"client", file.Name())

	slog.Debug("Le client a validé la reception avec ok")
	setIsCommandeUsed(false)
}

func handleHide(w *bufio.Writer, r *bufio.Reader, pathDir string, filename string, timeout time.Duration) {
	start := time.Now()
	filePath := filepath.Join(pathDir, filename)
	// Vérifie l'existence et le type du fichier
	fileInfo, err := os.Stat(filePath)

	if os.IsNotExist(err) || err != nil || (!fileInfo.Mode().IsRegular() && !fileInfo.IsDir()) {
		slog.Warn("Tentative de cacher un élément inconnu ou non régulier/répertoire : " + filename)
		w.WriteString("FileUnknown\n")
		w.Flush()
		return
	}

	//Ajoute le fichier a la liste des fichiers cachés
	hiddenFilesMux.Lock()
	defer hiddenFilesMux.Unlock()

	if _, alreadyHidden := hiddenFiles[filename]; !alreadyHidden {
		hiddenFiles[filename] = true
		slog.Info("Element caché: " + filename)
	}
	if time.Since(start) > timeout {
		w.WriteString("Timeout atteint\n")
		w.Flush()
		return
	}
	// Répondre OK
	w.WriteString("OK\n")
	w.Flush()
}

func handleReveal(w *bufio.Writer, r *bufio.Reader, pathDir string, filename string, timeout time.Duration) {
	start := time.Now()
	filePath := filepath.Join(pathDir, filename)

	//Vérifie l'existence sur le disque
	fileInfo, err := os.Stat(filePath)

	if os.IsNotExist(err) || err != nil || (!fileInfo.Mode().IsRegular() && !fileInfo.IsDir()) {
		slog.Warn("Tentative de révéler un élément inconnu ou non régulier/répertoire : " + filename)
		w.WriteString("FileUnknown\n")
		w.Flush()
		return
	}

	// Retire le fichier de la liste des fichiers cachés
	hiddenFilesMux.Lock()
	defer hiddenFilesMux.Unlock()

	if _, wasHidden := hiddenFiles[filename]; wasHidden {
		delete(hiddenFiles, filename) // Supprime l'entrée de la map
		slog.Info("Element révélé: " + filename)
	} else {
		slog.Debug("L'éelement " + filename + " n'était pas marqué comme caché.")
	}
	if time.Since(start) > timeout {
		w.WriteString("Timeout atteint\n")
		w.Flush()
		return
	}
	//Répond OK
	w.WriteString("OK\n")
	w.Flush()
}

func handleTerminate(w *bufio.Writer, r *bufio.Reader, timeout time.Duration) {
	start := time.Now()
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
	/* clientWG.Wait() */
	slog.Info("Tous les clients sont déconnectés. Serveur arrêté.")

	activeConnectionsMux.Lock()
	// Créer une liste de connexions à fermer pour éviter les deadlocks si la map est modifiée ailleurs
	connectionsToClose := make([]net.Conn, 0, len(activeConnections))
	for conn := range activeConnections {
		connectionsToClose = append(connectionsToClose, conn)
	}
	activeConnectionsMux.Unlock()

	for conn, _ := range activeConnections {
		conn.Close()
		// L'appel à conn.Close() ici déclenche la fonction defer dans handleClient/handleAdminClient
		// qui va décrémenter clientWG, mais nous avons déjà attendu clientWG.
	}
	slog.Info(fmt.Sprintf("%d connexions fermées par le serveur.", len(connectionsToClose)))
	// Vider la map pour ne pas garder de références (même si os.Exit(0) arrive)
	/* activeConnections = make(map[net.Conn]bool)
	   activeConnectionsMux.Unlock() */
	clientWG.Wait()
	if time.Since(start) > timeout {
		w.WriteString("Timeout atteint\n")
		w.Flush()
		return
	}
	//Envoie la confirmation au client de contrôle
	w.WriteString("OK\n")
	w.Flush()

	// Arrêt du processus
	/* serverWG.Wait() */
	slog.Info("Processus serveur terminé.")
	os.Exit(0) // Arrêter le processus Go
}
