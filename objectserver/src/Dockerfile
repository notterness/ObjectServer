FROM openjdk:13
COPY ./out/objectserver_jar /usr/src/myapp
WORKDIR /usr/src/myapp
CMD ["java", "-jar", "objectserver.jar", "5001", "docker"]
EXPOSE 3306
