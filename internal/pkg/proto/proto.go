package proto

import (
	"strings"
)

/**
*  La fonction Parser permet de parser une commande reçue
*  Elle retourne la commande, les flags et les valeurs associées
*  ainsi qu'une erreur si la commande est mal formée
*
*  @param in : la commande reçue
*
*  @return cmd : la commande
*  @return flags : les flags associés à la commande
*  @return values : les valeurs associées aux flags
*  @return err : true si la commande est mal formée, false sinon
*/
func Parser(in string) (cmd string, flags []string, values []string, err bool){

	// Split the input string into parts
	parts := strings.Fields(in)
	count := len(parts)

	// Check if there is at least one part (the command)
	if (count < 1) {
		err = true
		return
	}
	cmd = parts[0]

	// If there are no flags or values, return
	if (count == 1){
		return 
	}

	// Extract flags and values
	for i := 1; i < count; i++ {
		part := parts[i]
		// Regarder si le part est un flag
		if strings.HasPrefix(part, "-") {
			flags = append(flags, part)
			// Verifier si la valeur associée au flag existe
			if i+1 < count && !strings.HasPrefix(parts[i+1], "-") {
				values = append(values, parts[i+1])
				i++ // Passer le prochain part car c'est une valeur
			} else {
				values = append(values, "") // Aucune valeur associée au flag (-d)
			}
		} else {
			// Si un part ne commence pas par '-', la commande est mal formée
			err = true
			return
		}
	}

	return 

}
