package com.webutils.storageserver;

import com.webutils.storageserver.requestcontext.StorageServerContextPool;
import com.webutils.webserver.memory.MemoryManager;
import com.webutils.webserver.mysql.DbSetup;
import com.webutils.webserver.niosockets.NioServerHandler;
import com.webutils.webserver.requestcontext.WebServerFlavor;

import java.util.concurrent.atomic.AtomicInteger;

// Server class
public class StorageServerMain {
    public static void main(String[] args) {
        int baseTcpPort = DbSetup.storageServerTcpPort;
        WebServerFlavor flavor = WebServerFlavor.INTEGRATION_STORAGE_SERVER_TEST;


        final int NUMBER_TEST_STORAGE_SERVERS = 3;
        final int STORAGE_SERVER_BASE_ID_OFFSET = 100;

        if (args.length >= 1) {
            try {
                baseTcpPort = Integer.parseInt(args[0]);
            } catch (NumberFormatException ex) {
            }
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
                flavor = WebServerFlavor.DOCKER_STORAGE_SERVER_TEST;
            } else if (args[1].compareTo("kubernetes") == 0) {
                flavor = WebServerFlavor.KUBERNETES_STORAGE_SERVER_TEST;
            }
        }

        /*
        ** For the current time, the mock Storage Servers do not need to access the database records.
         */
        MemoryManager memoryManager = new MemoryManager(flavor);

        NioServerHandler[] nioStorageServer = new NioServerHandler[NUMBER_TEST_STORAGE_SERVERS];
        StorageServerContextPool[] requestContextPool = new StorageServerContextPool[NUMBER_TEST_STORAGE_SERVERS];

        for (int i = 0; i < NUMBER_TEST_STORAGE_SERVERS; i++) {
            requestContextPool[i] = new StorageServerContextPool(flavor, memoryManager, null);
            nioStorageServer[i] = new NioServerHandler(baseTcpPort + i,
                    (2000 + (i * STORAGE_SERVER_BASE_ID_OFFSET)), requestContextPool[i]);
            nioStorageServer[i].start();
        }
    }
}
