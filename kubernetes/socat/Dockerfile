FROM alpine
RUN apk --no-cache add socat
VOLUME /var/run/docker.sock
ENTRYPOINT ["socat"]
ENTRYPOINT ["socat", "TCP-LISTEN:1234,reuseaddr,fork","UNIX-CLIENT:/var/run/docker.sock"]
EXPOSE 1234 

