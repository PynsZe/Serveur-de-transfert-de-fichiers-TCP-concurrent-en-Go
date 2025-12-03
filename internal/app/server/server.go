package server

import (
	"bufio"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"
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

		if command == "List"{
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

}

func handleGet(w *bufio.Writer, r *bufio.Reader, pathDir string, filename string){

}