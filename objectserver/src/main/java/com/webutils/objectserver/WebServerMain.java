package com.webutils.objectserver;


import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.mysql.K8LocalDbInfo;
import com.webutils.webserver.mysql.TestLocalDbInfo;
import com.webutils.webserver.niosockets.NioServerHandler;
import com.webutils.webserver.requestcontext.WebServerFlavor;

/*
** If there is a parameter passed into the WebServerMain class, then it must be an integer
**   and it will be the port the WebServer (ObjectServer) listens on.
 */
public class WebServerMain {
    public static void main(String[] args) {
        int serverTcpPort;
        DbSetup dbSetup;

        WebServerFlavor flavor = WebServerFlavor.INTEGRATION_OBJECT_SERVER_TEST;

        if (args.length >= 1) {
            try {
                serverTcpPort = Integer.parseInt(args[0]);
            } catch (NumberFormatException ex) {
                serverTcpPort = 5001;
            }
        } else {
            serverTcpPort = 5001;
        }

        /*
         ** Check if this is a Docker or a Kubernetes image. The differences are:
         **   -> Both the Docker and Kubernetes images use "host.docker.internal" to
         **      access resources that are running on the same system, but outside of
         **      the Docker container. In the current code, this is just the MySQL database.
         **   -> For the Docker image, the "dockerServerIdentifier" MySQL table is used to
         **      lookup the addresses of the Storage Servers.
         **   -> For the normal and Kubernetes images, the "localServerIdentifier" MySQL table
         **      is used to lookup the addresses of the Storage Servers.
         */
        if (args.length == 2) {
            if (args[1].compareTo("docker") == 0) {
                flavor = WebServerFlavor.DOCKER_OBJECT_SERVER_TEST;
            } else if (args[1].compareTo("kubernetes") == 0) {
                flavor = WebServerFlavor.KUBERNETES_OBJECT_SERVER_TEST;
            }
            dbSetup = new K8LocalDbInfo(flavor);
        } else {
            dbSetup = new TestLocalDbInfo(flavor);
        }

        /*
        ** If this Object Server is running as a Docker Image or within a Kubernetes POD, it will need to access the
        **   database to obtain IP addresses.
         */
        dbSetup.checkAndSetupStorageServers();

        for (String s: args) {
            System.out.println(s);
        }

        NioServerHandler nioServer = new NioServerHandler(flavor, serverTcpPort, 1000, dbSetup);
        nioServer.start();
    }
}
