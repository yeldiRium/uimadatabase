FROM java:8

WORKDIR /code

# Adding .jar file and dependencies
ADD target/libs /code/libs
ADD target/target.jar /code/target.jar
ADD src/main/resources /code/src/main/resources
ADD main.sh /code/main.sh

CMD ["./main.sh"]
