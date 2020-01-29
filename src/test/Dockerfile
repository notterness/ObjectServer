FROM openjdk:13
COPY ./out/WebServer_jar /usr/src/myapp
WORKDIR /usr/src/myapp
CMD ["java", "-jar", "WebServer.jar", "5010", "docker"]
EXPOSE 3306
