Komparative Analyse von Datenbank- und Indexiserungssystemen im Kontext des Natural Language Processing
============

What this is:
-------------

This is my bachelor thesis. The written thesis can be found [here](bachelor_thesis.pdf).

This project evaluates a number of different DBMSs and one indexing system in the context of Apache UIMA NLP.
Those are:

- Arango DB
- BaseX
- Cassandra
- Lucene/Solr
- Mongo DB
- MySQL
- Neo4J

The DBMSs are spun up in separate docker containers for ease of access.

Prerequisites:
--------------
```
java
maven
docker
docker-compose
pdflatex
```

How to use:
-----------
```
mvn clean package
docker-compose build
docker-compose up
./visualize.sh
```
This will compile and package the application, build and start up the docker 
containers and run the script at `/main.sh`.

Usually this is used for one evaluation-run on a given input set and has to be 
run multiple times with differing inputs to get meaningful results.

Configure said script to change the evaluation process.

Input Files:
------------
The input files have to reside in `~/nlpdbeval_data/input` and can be parameterized by editing
the `docker-compose.yml` file.

Output:
-------
Output will be written to `~/nlpdbeval_data/output`

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
