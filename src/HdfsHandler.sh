#!/bin/bash
#title           :HdfsHandler.sh
#description     :Script qui permet de lancer Un nombre de datanodes passé en paramètres et un NameNode
#author		 :Youssef Achenchabe
#version         :0.1 (version execution locale)
clear


echo "------------Execution du script------------"
echo "Nombre de DataNodes : " $1
echo
echo
echo "-----------Lancement du NameNode-----------"
echo
java hdfs.NameNode &
echo "--------------NameNode lancé---------------"
echo "---------Lancement de " $1 " DataNodes----------"
echo

for ((i=0 ; i < $1 ; i++))
    do java hdfs.DataNode &
     echo "--------------DataNode lancé---------------"
     echo
done
echo "------------fin de l'execution------------"
