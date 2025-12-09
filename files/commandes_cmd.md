# [~]$ ssh-keygen -t ed25519
Generating public/private ed25519 key pair.
Enter file in which to save the key (/var/home/E248268G/.ssh/id_ed25519): mathis
Enter passphrase for "mathis" (empty for no passphrase): 
Enter same passphrase again: 
Your identification has been saved in mathis
Your public key has been saved in mathis.pub
The key fingerprint is:
[~]$ l
bash: l : commande introuvable
[~]$ ls
Android  go  mathis  mathis.pub  reseau
[~]$ ssh-add mathis
Identity added: mathis (E248268G@u-inf-j-f104-26)
[~]$ cat mathis.pub 
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINcFJi1pqvMRYxX+nS28GhC+jA6bVKi/MB6KEo+oGrPR E248268G@u-inf-j-f104-26
[~]$