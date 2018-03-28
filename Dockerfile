FROM java:8

# For the visualization of generated statistics
RUN apt-get -q update
RUN apt-get -qy install texlive-latex-extra texlive-fonts-recommended

WORKDIR /code

# Adding .jar file and dependencies
ADD target/libs /code/libs
ADD target/target.jar /code/target.jar
ADD src/main/resources /code/src/main/resources
ADD main.sh /code/main.sh

CMD ["./main.sh"]
