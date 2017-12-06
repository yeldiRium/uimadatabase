FROM java:8 

# Install maven
RUN apt-get update
RUN apt-get install -y maven

WORKDIR /code

# Prepare by downloading dependencies
ADD pom.xml /code/pom.xml
RUN ["mvn", "dependency:resolve"]
RUN ["mvn", "verify"]

# Adding source, compile and package into jar with dependencies in lib folder
ADD src /code/src
RUN ["mvn", "clean", "package"]
# move libs and jar to code root
RUN mv /code/target/libs /code
RUN mv /code/target/target.jar /code
# remove source code and other superfluous stuff
RUN rm -rf /code/src
RUN rm /code/pom.xml
RUN rm -rf /code/target