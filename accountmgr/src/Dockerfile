FROM openjdk:13
COPY ./out/accountmgr_jar /usr/src/myapp
WORKDIR /usr/src/myapp
CMD ["java", "-jar", "accountmgr.jar", "5002", "kubernetes"]
EXPOSE 3306 5002
