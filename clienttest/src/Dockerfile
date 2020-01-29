FROM openjdk:13
COPY ./out/clienttest_jar /usr/src/myapp
WORKDIR /usr/src/myapp
CMD ["java", "-jar", "clienttest.jar", "5010", "docker"]
EXPOSE 3306