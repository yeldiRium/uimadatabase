#!/bin/bash
java -jar target.jar evaluate -e write -d ArangoDB
java -jar targat.jar evaluate -e read -d ArangoDB
java -jar targat.jar evaluate -e query -d ArangoDB
java -jar targat.jar evaluate -e calculate -d ArangoDB
java -jar targat.jar evaluate -e complex-query -d ArangoDB
java -jar target.jar evaluate -e write -d Neo4j
java -jar targat.jar evaluate -e read -d Neo4j
java -jar targat.jar evaluate -e query -d Neo4j
java -jar targat.jar evaluate -e calculate -d Neo4j
java -jar targat.jar evaluate -e complex-query -d Neo4j
java -jar target.jar visualize