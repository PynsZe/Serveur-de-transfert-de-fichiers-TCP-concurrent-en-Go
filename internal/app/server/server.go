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
	"sync" // Paquet pour la synchronisation (Mutex, WaitGroup)
	"time"
)

/* ====================================================
                       ÉTAT GLOBAL
==================================================== */

var (
	// CLIENT 
	clientCount      int			// pour compter le nombre de client ( non admin sur un serveur )
	clientCountMux   sync.Mutex      // Mutex pour protéger l'accès concurrent à `clientCount`


	//FICHIER-DIR CACHEE
	hiddenFiles      map[string]bool // bool pour dire si un ficher/dir est cachés : map[filename] -> true (fichiers cachés, hors .*)
	hiddenFilesMux   sync.Mutex      // Mutex pour protéger l'accès concurrent à `hiddenFiles`

	// TERMINATE
	terminateChan    chan struct{}   // Canal utilisé pour signaler l'arrêt à `RunServer` (fermé pour signaler)
	terminateRequest bool            // requete pour terminer le serveur

	serverWG         sync.WaitGroup  // WaitGroup pour attendre la fin de la goroutine `RunServer`
	clientWG         sync.WaitGroup  // WaitGroup pour attendre que tous les `handleClient` se terminent

	// COMMANDES ACTIVES
	isCommandeUsed    bool       // true si au moins une commande List/Get/Cd/Admin est en cours d'exécution
	isCommandeUsedMux sync.Mutex // Mutex pour protéger l'accès concurrent à `isCommandeUsed`

	// CONNECTIONS ACTIVES
	activeConnections    map[net.Conn]bool // liste des connexions actives 
	activeConnectionsMux sync.Mutex // Mutex pour protéger l'accès concurrent à `activeConnections`
	connBusy    map[net.Conn]bool	// Suivi de l'activité par connexion : map[connexion] -> true (connexion en train d'exécuter une commande)
	connBusyMux sync.Mutex // Mutex pour protéger l'accès concurrent à `connBusy`

	// POUR SERVEUR ADMIN
	adminActive    bool       // true si un client est connecté en tant qu'administrateur
	adminActiveMux sync.Mutex // Mutex pour protéger l'accès concurrent à `adminActive`

)

const (
	// CONST UTILES POUR BUILDTREE
	DirPrefix     = "├── " // Préfixe pour tous les éléments sauf le dernier
	LastDirPrefix = "└── " // Préfixe pour le dernier élément d'une liste
	ChildPrefix   = "│   " // Préfixe pour l'indentation des enfants (continue)
	EmptyPrefix   = "    " // Préfixe pour l'indentation des enfants (vide)
)

/* Compteur pour le nombre de client connecté, utilisé comme intervalle de temps */
const CountClient = 1 * time.Second

/* ====================================================
                       FONCTIONS UTILITAIRES
==================================================== */

/*
* displayClientCount affiche périodiquement le nombre de clients connectés au serveur.
* Cette fonction tourne dans une goroutine séparée.
 */
func displayClientCount() {
	for {
		// **SYNCHRONISATION (Mutex)** : Verrouille l'accès pour lire `clientCount`
		clientCountMux.Lock()
		currentCount := clientCount
		clientCountMux.Unlock()

		// Afficher le compte actuel
		slog.Info(fmt.Sprintf("Clients connectés: %d", currentCount))

		// Attendre
		time.Sleep(CountClient)
	}
}

/*
* setIsCommandeUsed définit si une commande est actuellement utilisée sur le serveur.
* Utilisé par les handlers de commande (List, Get, Cd, etc.).
 */
func setIsCommandeUsed(val bool) {
	// SYNCHRONISATION (Mutex) : Verrouille pour écrire dans `isCommandeUsed`
	isCommandeUsedMux.Lock()
	isCommandeUsed = val
	isCommandeUsedMux.Unlock()
}

/*
* setConnBusy définit si la connexion donnée est occupée par une commande en cours d'exécution.
 */
func setConnBusy(conn net.Conn, v bool) {
	// SYNCHRONISATION (Mutex) : Verrouille pour modifier `connBusy`
	connBusyMux.Lock()
	if connBusy == nil {
		connBusy = make(map[net.Conn]bool)
	}
	connBusy[conn] = v
	connBusyMux.Unlock()
}

/*
* getConnBusy retourne si la connexion donnée est occupée par une commande en cours d'exécution.
 */
func getConnBusy(conn net.Conn) bool {
	// SYNCHRONISATION (Mutex): Verrouille pour lire dans `connBusy`
	connBusyMux.Lock()
	v := connBusy[conn]
	connBusyMux.Unlock()
	return v
}

/* ====================================================
                       BOUCLE PRINCIPALE DU SERVEUR
==================================================== */

/**
* La fonction RunServer lance le serveur principal qui écoute les connexions clients.
*
* @param port : le port d'écoute du serveur
* @param dir : le répertoire racine des fichiers à servir
* @param duration : la durée maximale d'attente
 */
func RunServer(port *string, dir *string, duration time.Duration) {

	cleanedRootDir := filepath.Clean(*dir) // sert a nettoyer le repertoire

	// Initialisations des structures globales ( fichier cachés )
	if hiddenFiles == nil {
		hiddenFiles = make(map[string]bool)
	}

	// SYNCHRONISATION (Channel) : Crée le canal de terminaison
	terminateChan = make(chan struct{})
	
	// Initialisation des structures pour les connections
	activeConnections = make(map[net.Conn]bool)
	connBusy = make(map[net.Conn]bool)


	// SYNCHRONISATION (WaitGroup) : Ajoute 1 pour la goroutine `RunServer`
	serverWG.Add(1)

	// 1. Démarrage de l'écoute TCP
	l, e := net.Listen("tcp", ":"+*port)
	if e != nil {
		slog.Error(e.Error())
		return
	}

	// 2. Fermeture différée de l'écoute
	defer func() {
		l.Close()
		// SYNCHRONISATION (WaitGroup): Indique que RunServer est terminé
		serverWG.Done()
		slog.Debug("Stopped listening on port " + *port)
	}()
	slog.Debug("Now listening on port " + *port)
	slog.Info("Files coming from directory " + cleanedRootDir)

	// 3. Lancement des goroutines de surveillance
	go displayClientCount() // Goroutine pour l'affichage périodique
	go checkForTerminate(l) // Goroutine pour la vérification de Terminate

	// 4. Boucle d'acceptation des connexions
	for {
		// SYNCHRONISATION (Channel) : Utilise `select` pour écouter le canal de terminaison
		select {
		case <-terminateChan:
			// Si signale reçu de checkTerminate ou handleTermiante alors le serveur ne reçois plus de connexion.
			slog.Info("Signal de TERMINATE reçu. Arrêt de l'écoute.")
			return
		default:
			// Accepte de nouvelles connexions
			c, e := l.Accept()
			if e != nil {
				// Gère l'erreur si l'écoute est fermée
				if strings.Contains(e.Error(), "use of closed network connection") {
					return
				}
				slog.Error("Erreur, ne peut pas accepté" + e.Error())
				continue
			}
			/* lancement d'une nouvelle goroutine pour gérer le client */
			// SYNCHRONISATION (WaitGroup) : Incrémente pour la nouvelle goroutine client
			clientWG.Add(1) //  Incrémenter le WaitGroup avant de lancer la goroutine
			go handleClient(c, cleanedRootDir, duration)
		}
	}
}

/* ====================================================
                       GESTION DES CLIENTS
==================================================== */

/*
* La fonction handleClient gère la communication avec un client connecté.
* Chaque client est géré dans une goroutine distincte.
*
* @param c : la connexion réseau avec le client
* @param rootDir : le répertoire racine des fichiers à servir
 */
func handleClient(c net.Conn, rootDir string, duration time.Duration) {

	currentDir := rootDir // Répertoire courant propre à cette connexion

	// 1. Incrémentation du compteur de clients pour chaque nouvelle connexion hors admin
	// SYNCHRONISATION (Mutex) : Protège `clientCount`
	clientCountMux.Lock()
	clientCount++
	clientCountMux.Unlock()

	// 2. Ajout à la liste des connexions actives
	// SYNCHRONISATION (Mutex) : Protège `activeConnections`
	activeConnectionsMux.Lock()
	activeConnections[c] = true
	activeConnectionsMux.Unlock()
	slog.Info("Incoming connection from " + c.RemoteAddr().String())

	// 3. Logique de nettoyage à la fin de la goroutine (defer)
	defer func() {
		// Retire de la liste des connexions actives
		activeConnectionsMux.Lock()
		delete(activeConnections, c)
		activeConnectionsMux.Unlock()

		c.Close()
		slog.Info("Connexion closed for" + c.RemoteAddr().String())

		// Décrémente le compteur de clients quand un client se déconnecte
		clientCountMux.Lock()
		clientCount--
		clientCountMux.Unlock()

		// SYNCHRONISATION (WaitGroup): Indique que la goroutine client est terminée
		clientWG.Done()
	}()

	// Prépare les lecteurs/écrivains pour la connexion
	reader := bufio.NewReader(c)
	writer := bufio.NewWriter(c)

	// 4. Boucle de lecture des commandes
	for {
		// Lit une ligne de commande envoyée par le client
		commandLine, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				slog.Error("N'a pas pu lire la commande" + err.Error())
			}
			break // Sort de la boucle, déclenchant le `defer`
		}

		// Nettoie la commande reçue
		commandLine = strings.TrimSpace(commandLine)
		slog.Debug("Commande reçu :" + commandLine)

		partie := strings.Fields(commandLine)
		if len(partie) == 0 {
			slog.Info("Commande incomplete. Veuillez retaper la commande.")
			continue
		}
		commande := partie[0]

		// 5. Dispatch des commandes et fait appelle aux handlers appropriés
		/*--- COMMANDES POUR LISTER LE REPERTOIRE / FICHIERS ---*/
		if commande == "List" {
			handleList(c, writer, reader, currentDir, duration)
		} else if commande == "Get" {
			if len(partie) < 2 {
				slog.Warn("commande incomplete")
				continue
			}
			filename := partie[1]
			handleGet(c, writer, reader, currentDir, filename, duration)
		/*--- COMMANDES POUR SE DEPLACER DANS L'ARBORESCENCE ---*/
		} else if commande == "Cd" {
			if len(partie) < 2 {
				slog.Warn("commande incomplete: Cd")
				writer.WriteString("CommandIncomplete\n")
				writer.Flush()
				continue
			}
			targetDir := partie[1]
			// Met à jour le répertoire courant
			newDir := handleChangeDir(c, writer, currentDir, rootDir, targetDir, duration)
			currentDir = newDir
		/*--- COMMANDES POUR FERMER LA SESSION ---*/
		} else if commande == "End" {
			slog.Info("Client" + c.RemoteAddr().String() + "veut se deconnecter")
			break // Sort de la boucle
		} else {
			slog.Warn("command inconnue " + commandLine)
			writer.WriteString("UnknownCommand\n")
			writer.Flush()
		}
	}
}

/* ====================================================
                     HANDLERS DE COMMANDES
==================================================== */

/*
* La fonction handleList gère la commande List côté serveur
*
* Note : Utilise `setIsCommandeUsed` et `setConnBusy` pour le suivi d'activité.
 */
func handleList(c net.Conn, w *bufio.Writer, r *bufio.Reader, pathDir string, duration time.Duration) {
	// 0. Lancement du temps pour le timeout
	start := time.Now()

	// 1. Début de l'opération : Marque le serveur et la connexion comme occupés
	setIsCommandeUsed(true)
	defer setIsCommandeUsed(false)
	setConnBusy(c, true)
	defer setConnBusy(c, false)

	messages := []string{}

	// 2. Construction de l'arborescence (récursif grace a buildTree)
	err := buildTree(pathDir, "", &messages, true) // true indique que c'est la racine
	if err != nil {
		slog.Error("Erreur lors de la construction de l'arborescence: " + err.Error())
		w.WriteString("FileCnt 0\n")
		w.Flush()
		r.ReadString('\n') // Tente de consommer la réponse OK du client si elle arrive
		return
	}

	count := len(messages) // Nombre de lignes dans l'arborescence

	// 3. Envoi de l'en-tête "FileCnt X"
	countMsg := fmt.Sprintf("FileCnt %d\n", count)
	w.WriteString(countMsg)
	slog.Debug("Serveur envoi: " + strings.TrimSpace(countMsg))

	// 4. Envoi de le tree ligne par ligne
	for _, msg := range messages {
		w.WriteString(msg)
	}

	w.Flush()
	slog.Debug(fmt.Sprintf("%d lignes d'arborescence envoyées", count))

	// Vérification du temps mis par la fonction
	if time.Since(start) > duration {
		w.WriteString("Timeout atteint\n")
		w.Flush()
		return
	}

	// 5. Attente du OK par le client
	ok, err := r.ReadString('\n')
	if err != nil || strings.TrimSpace(ok) != "OK" {
		// Log l'erreur si le client n'envoie pas le OK attendu
		if err != nil {
			slog.Error("LE CLIENT N'A PAS RÉPONDU OK (erreur ou EOF): ", "err", err.Error())
		} else {
			slog.Error("LE CLIENT N'A PAS RÉPONDU OK (mauvaise réponse): ", "resp", strings.TrimSpace(ok))
		}
		return
	}
	slog.Debug("Le client a validé la réception avec OK")
}

/*
* La fonction handleGet gère la commande Get côté serveur (téléchargement).
*
* Note : Utilise `setIsCommandeUsed` et `setConnBusy` pour le suivi d'activité.
 */
func handleGet(c net.Conn, w *bufio.Writer, r *bufio.Reader, pathDir string, filename string, duration time.Duration) {
	// 0. Lancement du temps pour le timeout
	start := time.Now()

	//Marque le serveur et la connexion comme occupés
	setIsCommandeUsed(true)
	setConnBusy(c, true)
	defer setConnBusy(c, false)
	defer setIsCommandeUsed(false) // Placé ici pour s'assurer qu'il est appelé après setConnBusy(false)

	filePath := filepath.Join(pathDir, filename) // Chemin complet du fichier

	// 1. Vérification du statut caché du fichier
	// SYNCHRONISATION (Mutex) : Protège `hiddenFiles`
	hiddenFilesMux.Lock()
	_, isHidden := hiddenFiles[filename] // Vérifie si le fichier est dans la map des fichiers cachés
	hiddenFilesMux.Unlock()

	if isHidden {
		slog.Warn("Tentative de GET sur un fichier caché: " + filename)
		w.WriteString("FileHidden\n")
		w.Flush()
		return
	}

	// 2. Ouverture du fichier
	file, err := os.Open(filePath)
	if err != nil {
		// Gestion des erreurs d'ouverture (non trouvé, erreur serveur)
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

	// 3. Récupération de la taille
	fileInfo, _ := file.Stat()
	//Vérification que le fichier donné est vraiment un fichier
	if !fileInfo.Mode().IsRegular() {
		w.WriteString("NotAFile\n")
		w.Flush()
		return
	}
	fileSize := fileInfo.Size()

	/* 4. Envoi de l'en-tête de début et de la taille */
	w.WriteString("Start\n") // Indique le début du transfert
	w.Flush()
	w.WriteString(fmt.Sprintf("%d\n", fileSize)) // Envoie la taille du fichier
	w.Flush()

	/* 5. Copie du contenu du fichier vers le réseau */
	n, err := io.Copy(w, file) // io.Copy est optimisé pour les transferts du fichier vers le buffer réseau
	w.Flush() // Force l'envoi

	if err != nil || n != fileSize {
		slog.Error("Transfert incomplet ou échoué",
			"fichier", filename,
			"envoyé", n,
			"attendu", fileSize,
			"erreur", err)
		return
	}
	slog.Debug(fmt.Sprintf("Transfert terminé %d octets envoyés", n))

	// Vérification du temps mis par la fonction
	if time.Since(start) > duration {
		w.WriteString("Timeout atteint\n")
		w.Flush()
		return
	}
	/* 6. Attente du OK par le client */
	ok, err := r.ReadString('\n')
	if err != nil || strings.TrimSpace(ok) != "OK" {
		if err != nil {
			slog.Error("LE CLIENT N'A PAS REPONDUS OK: ", "err", err.Error())
		} else {
			slog.Error("LE CLIENT N'A PAS REPONDUS OK (mauvaise réponse): ", "resp", strings.TrimSpace(ok))
		}
		return
	}

	slog.Info("Fichier téléchargé",
		"nom", filename,
		"taille", n,
		"client", file.Name())

	slog.Debug("Le client a validé la reception avec ok")
}

/* ====================================================
                     COMMANDES ADMIN
==================================================== */

/*
* La fonction handleHide gère la commande Hide côté serveur
*
* Note : Protège `hiddenFiles` avec `hiddenFilesMux`.
 */
func handleHide(w *bufio.Writer, r *bufio.Reader, pathDir string, filename string, duration time.Duration) {
	// 0. Lancement du temps pour le timeout
	start := time.Now()

	//  Vérifie l'existence du fichier
	filePath := filepath.Join(pathDir, filename) // Chemin complet du fichier
	fileInfo, err := os.Stat(filePath)

	if os.IsNotExist(err) || err != nil || (!fileInfo.Mode().IsRegular() && !fileInfo.IsDir()) { // Vérifie si le fichier existe et est régulier ou répertoire
		slog.Warn("Tentative de cacher un élément inconnu ou non régulier/répertoire : " + filename)
		w.WriteString("FileUnknown\n")
		w.Flush()
		return
	}

	// 1. Ajoute le fichier a la liste des fichiers cachés
	// SYNCHRONISATION (Mutex) : Protège `hiddenFiles`
	hiddenFilesMux.Lock()
	defer hiddenFilesMux.Unlock()

	if _, alreadyHidden := hiddenFiles[filename]; !alreadyHidden { // Évite de ré-ajouter si déjà caché
		hiddenFiles[filename] = true // Ajoute à la map des fichiers cachés
		slog.Info("Element caché: " + filename)
	}
	// Vérification du temps mis par la fonction
	if time.Since(start) > duration {
		w.WriteString("Timeout atteint\n")
		w.Flush()
		return
	}

	// 2. Répondre OK
	w.WriteString("OK\n")
	w.Flush()
}

/*
* La fonction handleReveal gère la commande Reveal côté serveur
*
* Note : Protège `hiddenFiles` avec `hiddenFilesMux`.
 */
func handleReveal(w *bufio.Writer, r *bufio.Reader, pathDir string, filename string, duration time.Duration) {
	// 0. Lancement du temps pour le timeout
	start := time.Now()

	//  Vérifie l'existence du fichier
	filePath := filepath.Join(pathDir, filename) // Chemin complet du fichier
	fileInfo, err := os.Stat(filePath) // Récupère les infos du fichier

	if os.IsNotExist(err) || err != nil || (!fileInfo.Mode().IsRegular() && !fileInfo.IsDir()) { // Vérifie si le fichier existe et est régulier ou répertoire
		slog.Warn("Tentative de révéler un élément inconnu ou non régulier/répertoire : " + filename) 
		w.WriteString("FileUnknown\n")
		w.Flush()
		return
	}

	// 1. Retire le fichier de la liste des fichiers cachés
	// SYNCHRONISATION (Mutex) : Protège `hiddenFiles`
	hiddenFilesMux.Lock()
	defer hiddenFilesMux.Unlock()

	if _, wasHidden := hiddenFiles[filename]; wasHidden { // Vérifie si le fichier était caché
		delete(hiddenFiles, filename) // Supprime l'entrée de la map
		slog.Info("Element révélé: " + filename)
	} else {
		slog.Debug("L'éelement " + filename + " n'était pas marqué comme caché.")
	}
	// Vérification du temps mis par la fonction
	if time.Since(start) > duration {
		w.WriteString("Timeout atteint\n")
		w.Flush()
		return
	}

	// 2. Répond OK
	w.WriteString("OK\n")
	w.Flush()
}

/*
* La fonction checkForTerminate vérifie périodiquement si une requête de terminaison a été faite
* et ferme le listener si aucune commande n'est en cours d'exécution.
* Cette fonction tourne dans une goroutine séparée.
*
* @param l : le listener réseau du serveur
 */
func checkForTerminate(l net.Listener) {
	for {
		if terminateRequest { // Si une requête de terminaison a été faite
			// SYNCHRONISATION (Mutex): Protège la lecture de `isCommandeUsed`
			isCommandeUsedMux.Lock()
			commandInProgress := isCommandeUsed // Vérifie si une commande est en cours
			isCommandeUsedMux.Unlock()

			if !commandInProgress { // Si aucune commande n'est en cours
				slog.Info("Aucune commande utilisée : on ferme le serveur (via checkForTerminate)")
				// Fermer le listener pour arrêter `RunServer`
				l.Close()
				// SYNCHRONISATION (Channel) : Signaler à `RunServer` de sortir de la boucle `select`
				select {
				case <-terminateChan: // Si déjà fermé, on ne fait rien
				default:
					close(terminateChan) // Ferme le canal de terminaison
				}
				return
			}
		}
		time.Sleep(100 * time.Millisecond) // Vérifier toutes les 100ms
	}
}

/*
* La fonction handleTerminate gère la commande Terminate côté serveur
*
* Elle lance un arrêt ordonné : arrête l'écoute, puis ferme progressivement les clients
* qui ne sont pas occupés par une commande.
 */
func handleTerminate(adminConn net.Conn, w *bufio.Writer) {

	slog.Warn("COMMANDE TERMINATE REÇUE. Début de l'arrêt ordonné.")

	terminateRequest = true // Met à jour le flag

	// 1. Stopper l'écoute de RunServer
	// SYNCHRONISATION (Channel) : Ferme le canal de terminaison (une seule fois)
	select {
	case <-terminateChan: // Si le canal est
		// déjà fermé ( par checkForTerminate)
	default:
		close(terminateChan)
	}
	slog.Info("Arrêt de l'écoute des nouvelles connexions.")

	// 2. Boucle de déconnexion progressive des clients inactifs (sauf l'admin)
	for {
		// Snapshot des connexions actuelles (sauf l'admin)
		// SYNCHRONISATION (Mutex) : Protège `activeConnections`
		activeConnectionsMux.Lock()
		conns := make([]net.Conn, 0, len(activeConnections)) // Pré-allocation
		for conn := range activeConnections { // Parcours des connexions actives
			if conn == adminConn {
				continue
			}
			conns = append(conns, conn) // Ajoute à la liste temporaire
		}
		activeConnectionsMux.Unlock()

		// Fermer les connexions qui ne sont pas occupées
		for _, conn := range conns {
			if !getConnBusy(conn) { // Vérification du statut occupé
				slog.Debug("Fermeture de la connexion inactive : " + conn.RemoteAddr().String())
				_ = conn.Close()
			}
		}

		// Vérifier s'il reste des clients
		// SYNCHRONISATION (Mutex) : Protège `clientCount`
		clientCountMux.Lock()
		remaining := clientCount // Nombre de clients restants
		clientCountMux.Unlock()

		// Si seul l'admin reste, on peut sortir
		if remaining <= 1 {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	// 3. Tous les clients non-admin sont déconnectés ou en cours de finir: informer l'admin
	w.WriteString("OK\n")
	w.Flush()

	slog.Info("Tous les clients sont déconnectés. Serveur arrêté.")
	os.Exit(0) // Arrêt brutal du processus après l'arrêt ordonné
}

/* ====================================================
                     COMMANDE BONUS : CD & TREE
==================================================== */

/*
* La fonction buildTree construit récursivement l'arborescence des fichiers et répertoires
* en respectant les fichiers cachés.
*
* Note : Cette fonction est appelée dans `handleList` et Utilise `hiddenFilesMux` pour lire la liste des fichiers cachés.
 */
func buildTree(pathDir string, prefix string, messages *[]string, isRoot bool) error {

	// Lit le contenu du répertoire
	entries, err := os.ReadDir(pathDir)
	if err != nil {
		return err // Retourne l'erreur si la lecture échoue
	}

	// Sépare les répertoires et les fichiers pour un tri personnalisé
	dirs := []os.DirEntry{}
	files := []os.DirEntry{}

	// Filtrage des éléments cachés (via la map `hiddenFiles` et les fichiers `.`)
	// SYNCHRONISATION (Mutex) : Protège la lecture de `hiddenFiles`
	hiddenFilesMux.Lock()
	for _, entry := range entries { // Parcours des entrées du répertoire
		name := entry.Name()

		if strings.HasPrefix(name, ".") { // Ignore les fichiers/répertoires de base cachés
			continue
		}

		if _, isHidden := hiddenFiles[name]; isHidden { // Ignore les fichiers cachés via commande Hide
			continue
		}

		if entry.IsDir() { // Si c'est un répertoire
			dirs = append(dirs, entry) // Ajoute à la liste des répertoires
		} else if entry.Type().IsRegular() { // Si c'est un fichier régulier
			files = append(files, entry) 	// Ajoute à la liste des fichiers
		}
	}
	hiddenFilesMux.Unlock()
	sortedEntries := append(dirs, files...) // Réunit les répertoires et fichiers, répertoires en premier
	totalEntries := len(sortedEntries) // Nombre total d'entrées à traiter

	// Si c'est la racine, ajoute un point pour représenter le répertoire courant
	if isRoot {
		*messages = append(*messages, ".\n")
	}

	// Parcours des entrées pour construire l'arborescence
	for i, entry := range sortedEntries { 
		isLast := i == totalEntries-1 // Vérifie si c'est la dernière entrée
		// Détermine les préfixes en fonction de la position dans l'arborescence
		linePref := DirPrefix 
		nextPref := ChildPrefix 
		if isLast {
			linePref = LastDirPrefix
			nextPref = EmptyPrefix
		}

		name := entry.Name()
		var line string

		if entry.IsDir() { // Si c'est un répertoire
			line = fmt.Sprintf("%s%s%s\n", prefix, linePref, name) // Formate la ligne pour un répertoire
		} else {
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
		if entry.IsDir() { // Si c'est un répertoire
			newPath := filepath.Join(pathDir, name) // Nouveau chemin
			newPrefix := prefix + nextPref // Nouveau préfixe pour l'indentation

			if err := buildTree(newPath, newPrefix, messages, false); err != nil { 
				slog.Error(fmt.Sprintf("Erreur lecture répertoire %s: %s", newPath, err.Error()))
			}
		}
	}
	return nil
}

/*
* La fonction handleChangeDir gère la commande Cd (Change Directory) côté serveur
*
* Note : Utilise `setIsCommandeUsed` et `setConnBusy` pour le suivi d'activité.
 */
func handleChangeDir(c net.Conn, w *bufio.Writer, currentDir string, rootDir string, targetDir string, duration time.Duration) string {
	// 0. Lancement du temps pour le timeout
	start := time.Now()

	// Marque le serveur et la connexion comme occupés
	setIsCommandeUsed(true)
	defer setIsCommandeUsed(false)
	setConnBusy(c, true)
	defer setConnBusy(c, false)

	// 1. Vérifie l'absence de chemins absolus (sécurité)
	if filepath.IsAbs(targetDir) { // Chemin absolu interdit
		slog.Warn("Tentative d'utiliser un chemin absolu: " + targetDir)
		w.WriteString("AbsolutePathsForbidden\n")
		w.Flush()
		return currentDir
	}

	// 2. Calcule et nettoie le chemin cible
	absTarget := filepath.Join(currentDir, targetDir) // Résout le chemin relatif
	cleanTarget := filepath.Clean(absTarget) // Nettoie le chemin

	// Assure que le nouveau répertoire est toujours sous le répertoire racine
	rel, err := filepath.Rel(rootDir, cleanTarget)
	if err != nil || strings.HasPrefix(rel, "..") {
		slog.Warn("Tentative d'accès hors du répertoire racine: " + cleanTarget)
		w.WriteString("AccessDenied\n")
		w.Flush()
		return currentDir
	}

	slog.Debug("Tentative de changement de dir vers : " + cleanTarget)

	// 3. Vérifie l'existence et le type (doit être un répertoire)
	fileInfo, err := os.Stat(cleanTarget)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteString("DirectoryUnknown\n")
		} else {
			w.WriteString("ServerError\n")
		}
		w.Flush()
		return currentDir
	}

	if !fileInfo.IsDir() {
		w.WriteString("NotADirectory\n")
		w.Flush()
		return currentDir
	}
	// Vérification du temps mis par la fonction
	if time.Since(start) > duration {
		w.WriteString("Timeout atteint\n")
		w.Flush()
		return ""
	}

	// 4. Succès : met à jour `currentDir` et répond OK
	slog.Info(fmt.Sprintf("CWD mis à jour de %s à %s", currentDir, cleanTarget))
	w.WriteString("OK\n")
	w.Flush()

	return cleanTarget
}
