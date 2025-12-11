package proto

import (
	"fmt"
	"strings"
)

/**
* La fonction Parser permet d'analyser une commande entrée par l'utilisateur
* et de la décomposer en commande et valeur(s) associée(s)
*
* @param in : la commande entrée par l'utilisateur
* @return cmd : la commande extraite
* @return values : les valeurs associées à la commande
* @return err : une erreur si la commande est mal formée, nil sinon
 */

func Parse(in string) (cmd string, values string, err error) {
	parts := strings.Fields(in)

	switch len(parts) {
	case 0:
		return "", "", fmt.Errorf("empty input")

	case 1:
		if parts[0] == "Get" {
			return "", "", fmt.Errorf("'Get' requires a value")
		}
		if parts[0] == "Hide" {
			return "", "", fmt.Errorf("'Hide' requires a value")
		}
		if parts[0] == "Reveal" {
			return "", "", fmt.Errorf("'Reveal' requires a value")
		}
		if parts[0] == "Cd" {
			return "", "", fmt.Errorf("'Cd' requires a value")
		}
		return parts[0], "", nil

	case 2:
		if parts[0] == "Get" {
			return "Get", parts[1], nil
		}
		if parts[0] == "Hide" {
			return "Hide", parts[1], nil
		}
		if parts[0] == "Reveal" {
			return "Reveal", parts[1], nil
		}
		if parts[0] == "Cd" {
			return "Cd", parts[1], nil
		}
		return "", "", fmt.Errorf("unknown 2-part command")
	default:
		return "", "", fmt.Errorf("invalid input format")
	}
}
