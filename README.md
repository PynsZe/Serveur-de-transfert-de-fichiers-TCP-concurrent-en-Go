
# Travail à faire

## Avant-propos 
Lors de ce projet, vous devez développer des programmes permettant l'échange de fichiers via une connexion tcp.

Clients et serveurs échangent deux types de messages : des éléments de protocoles (commandes ou réponses) et des données (le contenu des fichiers).
Les éléments de protocoles se terminent toujours par un retour à la ligne.
Les données sont envoyées sous forme d'un flux binaire.

## Contraintes
  - Le serveur doit pouvoir répondre à plusieurs clients simultanéments.
  - Le code ne doit pas comporter de _race condition_.
  - Le serveur doit _logger_ les informations relatives à chaque connexion : date de la connexion et adresse du client, liste des fichiers téléchargés, date de la déconnexion.
  - Le serveur doit afficher "en temps réel" le nombre de client connecté.
  - Lorsque le mode `debug` est activé, client et serveur doivent afficher l'ensemble des informations pertinentes pour décrire leur exécution.
  - L'ensemble des erreurs doivent être traitées.
  - Le code doit être documenté.


## Liste des commandes, partie 1
Ces commandes sont envoyées par le client au serveur. 
Pour chaque commande, les réponses possibles du serveur sont données.

### List 
Sur réception de la commande `List`, le serveur retourne la liste des fichiers disponibles :
  - `FileCnt` suivi d'une espace, suivi d'un nombre entier correspondant au nombre de fichiers disponibles.
  - puis, pour chaque fichier, son nom, suivi d'une espace, suivi d'un nombre entier correspondant à sa taille en octets.

Par exemple : 

``` 
FileCnt 5
a.txt 5 
b.txt 17
data.csv 3456
mesmotsdepassessecrets.odt 65665
script.sh 345
``` 

Après avoir reçu le nom et la taille du dernier fichier de la liste, le client répond par `OK`.

Les fichiers disponibles sont ceux présents dans un dossier dont le chemin est passé en paramètre lors du lancement du serveur.

### Get 
Sur réception de la commande `Get <filename>` où `<filename>` est le nom d'un fichier, le serveur répond :

  - `FileUnknown` si le nom du fichier est inconnu ;
  - ou bien il transfert le fichier sous le format suivant :
    - `Start`  
    - puis le flux binaire correspondant au format du fichier

Une fois qu'il a reçu soit la réponse `FileUnknown`, soit la totalité des octets attendus, le client envoie la réponse `OK`.

Dans le cas où le contenu du fichier a été correctement reçu, le programme client doit créer un fichier du même nom dans le dossier de travail du processus et y sauver le contenu reçu.

### End
Sur réception de la commande `End`, le serveur déconnecte le client.


## Liste des commandes, partie 2
Le serveur écoute les connexions entrantes sur un second port. 
Il accepte une seule connexion à la fois sur ce port. 
Sur cette connexion de contrôle, il répond à la commande `List`, mais pas à la commande `Get`.
Il doit répondre par ailleurs aux commandes ci-dessous.
Dans un premier temps, simulez le client de contrôle en utilisant le programme `nc` (déjà utilisé dans les séances précédentes).
Si vous avez le temps, vous pouvez ensuite en faire une implémentation.

### Hide
La commande `Hide <filename>` cache un fichier de la liste des fichiers disponbles. 
Si aucun fichier existant ne porte le nom demandé, le serveur répond par `FileUnknown`. 
Sinon, il supprime le fichier de la liste et confirme par `OK`.
Attention, le fichier n'est pas supprimé du système de fichier, seulement caché (il n'apparaît plus dans la liste).

### Reveal
La commande `Reveal <filename>` révèle un fichier qui avait été préalablement caché. 
Si le nom du fichier n'existe pas, la réponse est `FileUnknown`.
Sinon, le fichier est à nouveau ajouté à la liste et le serveur confirme par `OK`.

### Terminate
La commande `Terminate` enclenche la terminaison du serveur. 
Les réponses en cours aux commandes `List` et `Get` sont terminées (jusqu'à réception par le serveur de la confirmation), puis le serveur se déconnecte des clients. 
Une fois l'ensemble des clients déconnectés, une confirmation `OK` est envoyée au client de contrôle, puis le processus serveur est arrêté.

## Fonctionnalités optionnelles
Les fonctionnalités ci-dessous sont optionnelles, vous pouvez les ajouter si vous avez le temps (dans l'ordre que vous voulez).

### Servir une arborescence
Au lieu de servir une simple liste de fichiers, le serveur propose aux clients une arborescence contenant des fichiers et des dossiers.
Il faut adapter le protocole pour permettre aux clients (y compris le client de contrôle) de se _déplacer_ dans cette arborescence, pour pouvoir manipuler les fichiers.
Les commandes `Hide` et `Reveal` peuvent être étendues pour cacher/réveler des dossiers.

### Timeout
Pour éviter de bloquer des ressources inutilement en cas de plantage d'un client ou du serveur, les opérations d'envoi/réception de message peuvent être modifiées pour intégrer un délai maximal qui pourra être passé en ligne de commande.
Si une opération n'est pas réalisée dans le délai imparti, un message d'erreur est affiché dans les logs de l'application et la connexion est fermée.


# Présentation de ce qui est fourni

Pour démarrer le travail, vous partez du code fourni :
  - un serveur qui accepte les connexions tcp sur le port `3333` de la machine hôte, et qui se déconnecte au bout de 10 secondes ;
  - un client qui se connecte par défaut en tcp sur le port `3333` de la machine hôte, puis qui se termine.


## Organisation de l'espace de travail 

L'espace de travail est organisé comme suit :

```bash
.
├── cmd
│   ├── client
│   │   └── main.go
│   └── server
│       └── main.go
├── docs
├── go.mod
├── internal
│   ├── app
│   │   ├── client
│   │   │   └── client.go
│   │   └── server
│   │       └── server.go
│   └── pkg
│       └── proto
└── README.md

11 directories, 6 files
```

Cette organisation s'appuie sur l'organisation décrite [https://github.com/golang-standards/project-layout/tree/master](ici).

Le dossier `cmd` contient une première version des programmes principaux des deux commandes de la partie 1 : `server`, qui lance le serveur, et `client` qui lance le client.
A priori vous n'avez pas de fichier à ajouter dans les dossiers `cmd/client` et `cmd/server`.
Vous devrez modifier les fichiers `main.go` seulement si vous ajoutez des paramètres à vos commandes.

Le dossier `internal/app` contient le code des applications.
Le dossier `internal/app/client` contient le code relatif au client, et `internal/app/server` celui du serveur.
Les noms des _packages_ correspondants sont `gitlab.univ-nantes.fr/iutna.info2.r305/proj/internal/app/client` et `iutna.r305/proj/internal/app/server`.
Vous pouvez modifier les fichiers de ces dossiers ou en ajouter en fonction de vos besoins.

Le dossier `internal/pkg` peut être utilisé pour ajouter du code qui serait commun aux deux applications. 
À titre d'exemple, un dossier `proto` a été ajouté, dont le nom fait référence au protocole de communication entre client et serveur.
Pour importer ce module, le nom a utilisé sera : `gitlab.univ-nantes.fr/iutna.info2.r305/internal/pkg/proto`.
Une fois qu'un symbole a été ajouté, il faut utiliser la commande `go mod tidy` à la racine du projet pour mettre à jour les dépendances.
Une fois que c'est fait, les symboles publics du modules (ceux dont le nom commence par une majuscule) peuvent ensuite être utilisés, par exemple : `proto.FooBar()`.


## Construction des programmes

Pour construire le serveur, se placer à la racine du projet et utiliser la commande suivante :

```bash
go build -C cmd/server
```

Si la construction réussit, le fichier exécutable `cmd/server/server` est créé.

De la même façon, pour construire le client, se placer à la racine du projet et utiliser la commande suivante :

```bash
go build -C cmd/client
```

Si la construction réussit, le fichier exécutable `cmd/client/client` est créé.

