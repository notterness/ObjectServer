package com.webutils.chunkmgr;

import com.webutils.chunkmgr.requestcontext.ChunkAllocContextPool;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.K8ServersMgr;
import com.webutils.webserver.mysql.ServerIdentifierTableMgr;
import com.webutils.webserver.mysql.LocalServersMgr;
import com.webutils.webserver.niosockets.NioServerHandler;
import com.webutils.webserver.requestcontext.WebServerFlavor;

/*
** The DistributedShard client is responsible for managing where data is placed between the avaialble Storage Servers.
**
** For placement it uses the following information:
**   Customer
**   Object Prefix
**   Bucket Name
**   Object Name
**   StorageType -
**       Standard
**       Intelligent-Tiering
**       Standard-IA (Infrequent Access)
**       OneZone-IA
**       Archive
**       DeepArchive
**   Object UID - A 64 bit ID that uniquely identified an object being stored
**
** The path to the object is made up the following way:
**     ObjectPrefix/BucketName/ObjectName
*/
public class ChunkAllocMain {

    public static void main(String[] args) {
        int serverTcpPort;
        ServerIdentifierTableMgr serverTableMgr;

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
            serverTableMgr = new K8ServersMgr(flavor);
        } else {
            serverTableMgr = new LocalServersMgr(flavor);
        }

        for (String s: args) {
            System.out.println(s);
        }

        MemoryManager memoryManager = new MemoryManager(flavor);
        ChunkAllocContextPool requestContextPool = new ChunkAllocContextPool(flavor, memoryManager, serverTableMgr);

        NioServerHandler nioServer = new NioServerHandler(serverTcpPort, NioServerHandler.OBJECT_SERVER_BASE_ID, requestContextPool);
        nioServer.start();
    }
}

