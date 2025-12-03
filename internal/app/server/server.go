package server

import (
	"log/slog"
	"net"
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
	
	time.Sleep(10*time.Second)

}
