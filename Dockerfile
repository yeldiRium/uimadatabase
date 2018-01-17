FROM java:8

WORKDIR /code

# Adding .jar file and dependencies
ADD target/libs /code/libs
ADD target/target.jar /code/target.jar
ADD src/main/resources/config.yml /code/config.yml

CMD ["/usr/lib/jvm/java-8-openjdk-amd64/bin/java", "-jar", "/code/target.jar"]
