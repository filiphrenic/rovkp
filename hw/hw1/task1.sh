#! /usr/bin/env bash

##############
# 1. zadatak #
##############

mkdir ROVKP_DZ1
cd ROVKP_DZ1

start-dfs.sh
hdfs dfs -ls /user/rovkp
wget http://svn.tel.fer.hr/gutenberg.zip
hdfs dfs -copyFromLocal gutenberg.zip /user/rovkp

hdfs fsck /user/rovkp/gutenberg.zip
# sastoji se od 2 bloka
# replikacijski faktor je 1
# nije prilagodena prirodi HDFS-a, premala je

hdfs dfs -copyFromLocal /user/rovkp/gutenberg.zip gutenberg-hdfs.zip
md5sum *
# iste su
