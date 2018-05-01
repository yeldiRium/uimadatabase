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

java -jar target.jar evaluate -e write -d Blazegraph
java -jar target.jar evaluate -e read -d Blazegraph
#java -jar target.jar evaluate -e query -d Blazegraph
#java -jar target.jar evaluate -e calculate -d Blazegraph
#java -jar target.jar evaluate -e complex-query -d Blazegraph
