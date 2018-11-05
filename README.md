
# Installations projet Spark

## 1. Installation de la VM Ubuntu bash
Sous Windows 10, se procurer la VM légère Ubuntu bash (une invite de commande Linux fonctionnant sous Windows). Elle est disponible depuis le menu **Paramètres -> Mise à jour et sécurité** et dans le menu "**Pour les développeurs**" , cocher le bouton "**Mode développeur**".
Taper « **fonctionnalités** » dans la barre de recherche et cliquer sur "**Activer ou désactiver des fonctionnalités Windows**" .
Cocher la case « _**Sous-système Windows pour Linux**_ » et faire OK.  L'ordinateur devra ensuite redémarrer.
Eventuellement configurer l'environnement (je ne sais plus si c'est nécessaire) via la nouvelle application "Ubuntu".

Pour ensuite accéder au shell Ubuntu, utiliser la commande `bash`depuis un invite de commandes.

Plus d'info sur https://www.howtogeek.com/249966/how-to-install-and-use-the-linux-bash-shell-on-windows-10/

Remarque : l'arborescence de fichiers Windows est accessible depuis le bash, aux chemins d'accès `/mnt/c` pour le lecteur C, `/mnt/d` pour le D, etc.
## 2. Installation des paquets nécessaires à Spark
Télécharger Java, Python et Git sur la VM depuis le bash : 
`sudo apt-get install java`
`sudo apt-get install python`
`sudo apt-get install git`

En cas d'erreur de dépendances de paquets, quelques commandes utiles : 
`sudo apt-get update --fix-missing`
`sudo apt-get upgrade`
## Mise en place du repository Git
Créer un dossier pour le projet puis y accéder et lancer les commandes Git pour initialiser le repository.
`git init`
`git clone https://github.com/gdouar/5IFsysdistribues `
puis `git pull`.

## Installation de Spark
Toujours depuis la VM, installer Spark et SBT (pour compiler des programmes Scala).
### Spark
Le lien :  https://spark.apache.org/downloads.html
Installer la dernière version. Pour cela télécharger l'archive TAR de Spark dans le dossier du projet.
A. Dézipper l'archive ainsi depuis le bash : `tar xvf <nom_archive>` (ou depuis Winrar ou autre logiciel)
B. Accéder à ce dossier (`cd nom_archive`) puis lancer les commandes :
`mkdir /usr/local/spark` puis 
 `sudo mv ./* /usr/local/spark` pour bouger tout son contenu dans un dossier fixe. 
C. Enfin lancer la commande `sudo nano ~/.bashrc` pour ajouter le dossier des commandes binaires de Spark  dans le script de chargement de la console (pour pouvoir les utiliser depuis n'importe où). Pour cela, descendre tout en bas du fichier et ajouter la ligne `export PATH=$PATH:/usr/local/spark/bin` à la fin. Sauvegarder le tout en faisant Ctrl+X, puis Y et Entrée, et ré exécuter le script via la commande `source  ~/.bashrc `.

### SBT
Effectuer exactement les mêmes étapes que le point précédent, en remplaçant `spark`par `sbt`dans les chemins d'accès, avec l'archive suivante : https://piccolo.link/sbt-1.2.6.tgz. 

### Vérifier que tout est bien installé
Pour vérifier les différentes installations, tester les commandes suivantes depuis le dossier du projet : 
`java -version`
`python -v`
`sbt sbtVersion`
`spark-shell`

# Prise en main du projet

## Lancer le projet
Pour compiler + lancer le projet, un script shell exécutable se trouve à sa racine, `compileAndExecScala.sh` (à exécuter avec le bash). Si les paramètres de logging Java sont par défaut sur la machine (cf point suivant), exécuter plutôt le fichier `compileAndExecScalaNoCustomLog.sh`(à confirmer).
Au 1er lancement la commande de compilation télécharge toutes les dépendances, puis le programme de test SimpleApp.scala est exécuté.

## Logging
Le niveau de logging sur Spark est par défaut très élevé (INFO). Pour limiter les sorties console on peut le paramétrer depuis la configuration de Spark. Pour cela, deux commandes sont à effectuer : 

`sudo cp /usr/local/spark/log4j.properties.template /usr/local/spark/log4j.properties` : copie d'un fichier "template" fourni par défaut par Spark pour la gestion du logger java.

`sudo nano /usr/local/spark/log4j.properties` : remplacer dans ce nouveau fichier la ligne `log4j.rootCategory=INFO`, par  `log4j.rootCategory=ERROR`, puis Ctrl+X et Y pour sauvegarder.



