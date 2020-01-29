package com.webutils.objectserver;


import com.webutils.webserver.niosockets.NioServerHandler;
import com.webutils.webserver.requestcontext.WebServerFlavor;

/*
** If there is a parameter passed into the WebServerMain class, then it must be an integer
**   and it will be the port the WebServer (ObjectServer) listens on.
 */
public class WebServerMain {
    public static void main(String[] args) {
        int serverTcpPort;
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

        if (args.length == 2) {
            if (args[1].compareTo("docker") == 0) {
                flavor = WebServerFlavor.DOCKER_OBJECT_SERVER_TEST;
            }
        }

        for (String s: args) {
            System.out.println(s);
        }

        NioServerHandler nioServer = new NioServerHandler(flavor, serverTcpPort, 1000);
        nioServer.start();
    }
}
