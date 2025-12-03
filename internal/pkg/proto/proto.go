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

	if (count == 1){
		return 
	}

	for i:=1; i<(count-1)/2; i++ {

	}

	return 

}
