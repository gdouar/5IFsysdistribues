# Installations projet Spark
## 1. 
sudo apt-get install java
sudo apt-get install python

en cas d'erreur : 

sudo apt-get update --fix-missing
sudo apt-get upgrade

https://piccolo.link/sbt-1.2.6.tgz
https://spark.apache.org/downloads.html


tar xvf <nom_archive>
sudo mv <nom archive> /usr/local/spark ou /usr/local/sbt
sudo nano ~/.bashrc
ajouter 
export PATH=$PATH:/usr/local/spark/bin
puis
export PATH=$PATH:/usr/local/sbt/bin

Sauvegarder : Ctrl + X puis Y

puis 

source  ~/.bashrc


pour le logging : 
sudo cp /usr/local/spark/log4j.properties.template /usr/local/spark/log4j.properties
sudo nano /usr/local/spark/log4j.properties
remplacer   log4j.rootCategory=INFO, console par   log4j.rootCategory=ERROR, console
Ctrl+X et Y pour sauvegarder