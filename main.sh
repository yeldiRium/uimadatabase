#!/bin/bash
# To be run in the docker container. Contains hard coded paths that have to by
# synced with the environment variables set in docker-compose.yml
set -e

if [ ! -d /code/input ]
then
	mkdir /code/input
	echo "Please add some input files and set appropriate configuration values."
fi

if [ ! -d /code/output ]
then
	mkdir /code/output
fi

java -jar target.jar evaluate -e write -d ArangoDB
java -jar target.jar evaluate -e read -d ArangoDB
java -jar target.jar evaluate -e query -d ArangoDB
java -jar target.jar evaluate -e calculate -d ArangoDB
java -jar target.jar evaluate -e complex-query -d ArangoDB

java -jar target.jar evaluate -e write -d BaseX
java -jar target.jar evaluate -e read -d BaseX
java -jar target.jar evaluate -e query -d BaseX
java -jar target.jar evaluate -e calculate -d BaseX
java -jar target.jar evaluate -e complex-query -d BaseX

java -jar target.jar evaluate -e write -d MySQL
java -jar target.jar evaluate -e read -d MySQL
java -jar target.jar evaluate -e query -d MySQL
java -jar target.jar evaluate -e calculate -d MySQL
java -jar target.jar evaluate -e complex-query -d MySQL

java -jar target.jar evaluate -e write -d Neo4j
java -jar target.jar evaluate -e read -d Neo4j
java -jar target.jar evaluate -e query -d Neo4j
java -jar target.jar evaluate -e calculate -d Neo4j
java -jar target.jar evaluate -e complex-query -d Neo4j

java -jar target.jar visualize
