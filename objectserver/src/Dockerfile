FROM openjdk:13
COPY ./out/objectserver_jar /usr/src/myapp
WORKDIR /usr/src/myapp
ENV DOCKER_URI http://localhost:1234
CMD ["java", "-jar", "objectserver.jar", "5001", "kubernetes"]
EXPOSE 3306
