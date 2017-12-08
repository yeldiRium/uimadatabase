uimadatabase
============

What this is:
-------------

This project evaluates a number of different DBMSs in the context of apache UIMA NLP.
The DBMSs are:
- Arango DB
- BaseX
- Cassandra
- Mongo DB
- MySQL
- Neo4J

Still undecided:
- Microsoft Cosmos DB

The DBMSs are spun up in separate docker containers (if necessary and not externally hosted)
for ease of access.


How to use:
-----------
```
docker-compose build
docker-compose up
```
This will compile and package the application, build and start up the docker containers and run the Main class.
Currently this will start a test for all database server connections.