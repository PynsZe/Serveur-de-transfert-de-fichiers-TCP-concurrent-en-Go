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
	clientCount    int
	clientCountMux sync.Mutex // Mutex pour protéger clientCount
	hiddenFiles    map[string]bool // map[filename] -> true (caché)
    hiddenFilesMux sync.Mutex // Mutex pour protéger hiddenFiles
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

func RunServer(port *string, dir *string) {

	if hiddenFiles == nil {
        hiddenFiles = make(map[string]bool)
    }

	l, e := net.Listen("tcp", ":"+*port)
	if e != nil {
		slog.Error(e.Error())
		return
	}
	defer func() {
		l.Close()
		slog.Debug("Stopped listening on port " + *port)
	}()
	slog.Debug("Now listening on port " + *port)
	slog.Info("Files coming from directory " + *dir)

	/* compteur mis en route */
	go displayClientCount()

	for {
		c, e := l.Accept()
		if e != nil {
			slog.Error("Erreur, ne peut pas accepté" + e.Error())
			continue
		}
		/* lancement d'une nouvelle go routine pour le client */
		go handleClient(c, *dir)
	}
}

/* fonction pour gérer les commandes  des tout les clients : list get et end  */

func handleClient(c net.Conn, rootDir string){

	clientCountMux.Lock()
    clientCount++
    clientCountMux.Unlock()

	slog.Info("Incoming connection from " + c.RemoteAddr().String())
	defer func()  {
		c.Close()
		slog.Info("Connexion closed for" + c.RemoteAddr().String())

		clientCountMux.Lock()
        clientCount--
        clientCountMux.Unlock()

	}()

	/* boucle pour list get et end */
	
	/* time.Sleep(10*time.Second) */

	reader := bufio.NewReader(c)
	writer := bufio.NewWriter(c)

	for{
		commandLine, err  := reader.ReadString('\n')
		if err != nil{
			if err != io.EOF{
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
		commande := partie[0] /* commande */

		/* commandes */

		if commande == "List"{
			handleList(writer, reader, rootDir)
		} else if commande == "Get"{
			if len(partie) < 2{
				slog.Warn("commande incomplete")
				continue
			}
			filename := partie[1]  /* fichier */
			handleGet(writer, reader, rootDir, filename)
		} else if commande == "End"{
			slog.Info("Client" + c.RemoteAddr().String() + "veut se deconnecter")
			break
		} else {
			slog.Warn("command inconnue " + commandLine)
			writer.WriteString("UnknownCommand\n")
			writer.Flush()
		}
	
	}

}

func handleList(w *bufio.Writer, r *bufio.Reader, pathDir string){

	/* lecture  du contenue du dossier */

	entre, err := os.ReadDir(pathDir)
	if err != nil{
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

	for _, entree := range entre{
		if _, isHidden := hiddenFiles[entree.Name()]; isHidden {
            continue 
        }
		if entree.Type().IsRegular(){
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
	
	for _, msg := range messages{
		w.WriteString(msg)
		slog.Debug("Serveur envoi :" + strings.TrimSpace(msg))
	}

	w.Flush()
	slog.Debug(fmt.Sprintf("%d fichiers listés et envoyés", count))
	
	ok, err := r.ReadString('\n')
	if err != nil ||  strings.TrimSpace(ok) != "OK"{
		slog.Error("LE CLIENT N'A PAS REPONDUS OK")
		if err == io.EOF {
            return
        }
		return
	}
	slog.Debug("Le client a validé la reception avec ok")

}

func handleGet(w *bufio.Writer, r *bufio.Reader, pathDir string, filename string){

	filePath := filepath.Join(pathDir, filename)
	
	/* ouverture du fichier */
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err){
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

	/* copie du fichier vers le reseau */
	n, err := io.Copy(w, file)
	w.Flush()

	if err != nil || n != fileSize{
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
	if err != nil ||  strings.TrimSpace(ok) != "OK"{
		slog.Error("LE CLIENT N'A PAS REPONDUS OK")
		return
	}

	slog.Info("Fichier téléchargé", 
        "nom", filename, 
        "taille", n,
        "client", file.Name())

	slog.Debug("Le client a validé la reception avec ok")

}

func handleHide(w *bufio.Writer, r *bufio.Reader, pathDir string, filename string){

	filePath := filepath.Join(pathDir, filename)
<<<<<<< HEAD
	
	/* ouverture du fichier */
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err){
			slog.Warn("Fichier inconnue" + filename)
			w.WriteString("FileUnknown\n")
		} else {
			slog.Error("Erreur lors de l'ouverture du fichier" + err.Error())
			w.WriteString("ServerErreur\n")
		}
		w.Flush()
		return
	}
	_ = file // pour pas que go me les brisent
=======
	// Vérifie l'existence et le type du fichier
	fileInfo, err := os.Stat(filePath)
>>>>>>> refs/remotes/origin/main

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

func handleReveal(w *bufio.Writer, r *bufio.Reader, pathDir string, filename string){

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

func handleTerminate(w *bufio.Writer, r *bufio.Reader){
}