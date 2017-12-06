The credentials for the BaseXHTTP server are configured in the web.xml file.
They are by default set to root:root.

They are again set in the docker-compose.yml file, so that they can be accessed
in the java code and easily changed.
The values in web.xml and docker-compose.yml _must_ always be changed together,
otherwise the BaseXClient can't connect to the BaseXHTTP server.  