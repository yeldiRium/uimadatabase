FROM java:8 

# Install maven
RUN apt-get update
RUN apt-get install -y maven

WORKDIR /code

# Prepare by adding and downloading dependencies
ADD lib /code/lib
ADD pom.xml /code/pom.xml
RUN ["mvn", "dependency:resolve"]i
RUN ["mvn", "verify"]

# Adding source, compile and package into jar with dependencies in lib folder
ADD src /code/src
RUN ["mvn", "clean", "package"]
# move libs and jar to code root
RUN mv /code/target/libs /code
RUN mv /code/target/target.jar /code

# removing source code, tests and target folder
# but keeping resources
RUN rm -rf /code/src/main/java
RUN rm -rf /code/src/test
RUN rm -rf /code/target
RUN rm /code/pom.xml

# prepare output directory
ADD output /code/output