FROM openjdk:13
COPY ./out/storageserver_jar /usr/src/myapp
WORKDIR /usr/src/myapp
CMD ["java", "-jar", "storageserver.jar", "5010", "kubernetes"]
EXPOSE 3306 5010 5011 5012 5013
