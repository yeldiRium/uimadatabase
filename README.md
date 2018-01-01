uimadatabase
============

What this is:
-------------

This project evaluates a number of different DBMSs in the context of Apache UIMA NLP.
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

Prerequisites:
--------------
```
docker
docker-compose
```
Java, Maven etc. are not necessary, since they will be run inside the docker container.

How to use:
-----------
```
docker-compose build
docker-compose up
```
This will compile and package the application, build and start up the docker containers and run the Main class.
Currently this will start a test for all database server connections.

More Information on Dockerization:
----------------------------------
Docker-Compose simplifies the task to execute a number of complicated Docker commands.
Instead of remembering or writing down every single command to start the several containers
in this project (see subfolders in folder dbs), we have the `docker-compose.yml` config file
which tells Docker-Compose which commands to run.
So running `docker-compose build` as a user makes docker-compose run creation commands for the
several containers. It creates most of them from an existing image created by the dbms' developer.
Then some configuration is applied for authentication and the container is done.

Then `docker-compose up` runs all those containers in parallel and executes predefined tasks.
In most of the containers this task is starting the dbms' daemon. In the main container the task
is manually defined in `docker-compose.yml` as usually executing target.jar. What this does is
again defined in the Maven build configuration.

To run this an a production server one would:
Run `docker-compose build`. Then push the resulting image files in the docker registry to a
docker image repository. This can be the official one or a self-hosted solution.
On the production machine then docker is installed and the images are imported.
From there on I don't know what to do myself, since somehow the docker-compose config must come
into play. (If that isn't already the case after `docker-compose build`. Probaby just starting up
the individual images is enough.)
I have to look this up but it won't be relevant until the main components of the application work
anyway.
