package client

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
)

func Run(remote string) {

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
		fmt.Println("commande : ")
		line, _ := stdin.ReadString('\n')
        line = strings.TrimSpace(line)

		_, err := out.WriteString(line + "\n")
		if (err != nil){
			slog.Error(e.Error())
			return
		}	
		out.Flush()
		switch line{
			case "end":
				slog.Info("Session closed by user")
				return
			case "List":
				handleListClient(in,out)
				return
		}
		resp, err := in.ReadString('\n')
		if (err != nil){
			slog.Error(e.Error())
			return
		}

		fmt.Println(resp)
	}
}

func handleListClient(in *bufio.Reader,out *bufio.Writer) {
	_, err := out.WriteString("List\n")
	println(1)
	if err != nil {
		slog.Error("Erreur envoi OK : " + err.Error())
		return
	}
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
	fmt.Println("→ Envoi OK")
	_, err = out.WriteString("OK\n")
	if err != nil {
		slog.Error("Erreur envoi OK : " + err.Error())
		return
	}
	out.Flush()
}