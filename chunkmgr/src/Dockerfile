FROM openjdk:13
COPY ./out/chunkmgr_jar /usr/src/myapp
WORKDIR /usr/src/myapp
CMD ["java", "-jar", "chunkmgr.jar", "5002", "kubernetes"]
EXPOSE 3306 5002
