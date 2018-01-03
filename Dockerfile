FROM java:8 

# Install maven
RUN apt-get update
RUN apt-get install -y maven

WORKDIR /code

# Adding source, compile and package into jar with dependencies in lib folder
ADD lib /code/lib
ADD pom.xml /code/pom.xml
ADD src /code/src
RUN ["mvn", "clean", "package"]

# removing source code and tests
# but keeping main/resources
RUN rm -rf /code/src/main/java
RUN rm -rf /code/src/test

CMD ["/usr/lib/jvm/java-8-openjdk-amd64/bin/java", "-jar", "/code/target/target.jar"]