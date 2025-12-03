package server

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	//"time"
)

func RunServer(port *string, dir *string) {

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
	slog.Info("Files coming from directory" + *dir)

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
	slog.Info("Incoming connection from " + c.RemoteAddr().String())
	defer func()  {
		c.Close()
		slog.Info("Connexion closed for" + c.RemoteAddr().String())
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
		} else {
			slog.Warn("command inconnue" + commandLine)
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

	/* preparation du message pour filecnt + comptage des fichiers  */
	/* preparation d'une liste de messages de fichiers a envoyer */

	messages := []string{}

	for _, entree := range entre{
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
		return
	}
	slog.Debug("Le client a validé la reception avec ok")

}

func handleGet(w *bufio.Writer, r *bufio.Reader, pathDir string, filename string){

}