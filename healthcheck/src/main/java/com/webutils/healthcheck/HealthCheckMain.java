package com.webutils.healthcheck;

import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.http.HttpRequestInfo;
import com.webutils.webserver.http.TenancyUserHttpRequestInfo;
import com.webutils.webserver.manual.SendHealthCheck;
import com.webutils.webserver.mysql.*;
import com.webutils.webserver.requestcontext.ServerIdentifier;
import com.webutils.webserver.requestcontext.WebServerFlavor;
import org.eclipse.jetty.http.HttpField;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

public class HealthCheckMain {
    private static final String SERVICE_ARG = "-service";
    private static final String IP_ADDR_ARG = "-ip";
    private static final String PORT_ARG = "-port";

    private static final List<String> serviceNames = new LinkedList<>(List.of("chunk-mgr-service", "object-server",
            "storage-server-1", "storage-server-2", "storage-server-3", "storage-server-4"));


    public static void main(String[] args) {

        String service = null;
        /*
         ** This runs as an executable against the Kubernetes POD
         */
        ServerIdentifierTableMgr serverTableMgr = new K8PodServersMgr(WebServerFlavor.INTEGRATION_KUBERNETES_TESTS);

        /*
        ** The options for the command line arguments are:
        **    -ip - This is the IP address to send the request to
        **    -port - This when combined with the ip address determines where to send the health check request
        **    -service - Instead of using the ip and port, the name of the service can be specified instead.
         */
        InetAddress serviceIp = null;
        int servicePort = -1;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase(SERVICE_ARG)) {
                /*
                ** The next args[] should be the name of the service
                 */
                if ((i + 1) < args.length) {
                    if (serviceNames.contains(args[i + 1])) {
                        service = args[i + 1];
                    }
                }
            } else if (args[i].equalsIgnoreCase(IP_ADDR_ARG)) {
                /*
                 ** The next args[] should be the IP address of the service
                 */
                if ((i + 1) < args.length) {
                    try {
                        serviceIp = InetAddress.getByName(args[i + 1]);
                    } catch (UnknownHostException ex) {
                        System.out.println("-ip is not valid: " + args[i + 1] + " - " + ex.getMessage());
                    }
                }
            } else if (args[i].equalsIgnoreCase(PORT_ARG)) {
                /*
                 ** The next args[] should be the IP address of the service
                 */
                if ((i + 1) < args.length) {
                    try {
                        servicePort = Integer.parseInt(args[i + 1]);
                    } catch (NumberFormatException ex) {
                        System.out.println("-port is not a valid numeric format - " + args[i + 1]);
                    }
                }
            }
        }

        if (service != null) {
            /*
            ** Find the Ip address and Port of the service
             */
            List<ServerIdentifier> servers = new LinkedList<>();
            serverTableMgr.getServer(service, servers);
            if (servers.size() == 1) {
                ServerIdentifier server = servers.get(0);

                serviceIp = server.getServerIpAddress();
                servicePort = server.getServerTcpPort();
            } else {
                System.out.println("Number of services found is wrong: " + servers.size());
                return;
            }
        } else if ((serviceIp == null) || (servicePort == -1)) {
            System.out.println("Must have both -ip and -port defined");
            return;
        }

        System.out.println("IP: " + serviceIp.toString() + " port: " + servicePort);

        /*
         ** Obtain the token to send the Health Check commands
         */
        String adminName = "Administrator";
        String adminTenancyName = "AdministratorTenancy-Global";

        HttpRequestInfo httpHealthCheckInfo = new TenancyUserHttpRequestInfo();

        HttpField adminTenancyField = new HttpField(HttpInfo.TENANCY_NAME, adminTenancyName);
        httpHealthCheckInfo.addHeaderValue(adminTenancyField);
        HttpField adminCustomerField = new HttpField(HttpInfo.CUSTOMER_NAME, adminName);
        httpHealthCheckInfo.addHeaderValue(adminCustomerField);

        String healthCheckUserName = "HealthCheckAdmin@test.com";
        String healthCheckUserPassword = "HealthCheckAdmin_password";
        HttpField healthCheckUserField = new HttpField(HttpInfo.USER_NAME, healthCheckUserName);
        httpHealthCheckInfo.addHeaderValue(healthCheckUserField);
        HttpField healthCheckPasswordField = new HttpField(HttpInfo.USER_PASSWORD, healthCheckUserPassword);
        httpHealthCheckInfo.addHeaderValue(healthCheckPasswordField);

        UserTableMgr userMgr = new UserTableMgr(WebServerFlavor.INTEGRATION_TESTS);

        String healthCheckAccessToken = userMgr.getAccessToken(httpHealthCheckInfo);
        System.out.println("HealthCheck accessToken: " + healthCheckAccessToken);

        AtomicInteger threadCount = new AtomicInteger(1);

        /*
         ** Validate the Health Check for the specified service or IP/Port combination.
         */
        SendHealthCheck healthCheck = new SendHealthCheck(serviceIp, servicePort, healthCheckAccessToken, threadCount);
        healthCheck.execute();

        waitForTestsToComplete(threadCount);

        System.out.println("Health check shutting down");
    }

    /*
     ** This waits until the test have completed. When a test starts, it increments an atomic variable and
     **   decrements it when it completes. This allows the code to wait for a group of tests to complete
     **   prior to moving onto another set of tests or exiting.
     */
    static void waitForTestsToComplete(AtomicInteger testRunningCount) {
        // running infinite loop for getting
        // client request
        while (true) {
            int count = testRunningCount.get();

            /*
             ** The TestClient() cannot finish until after all the tests have completed so there is always
             **   a count of 1 even after all the tests have completed.
             */
            if (count != 1) {
                System.out.println("\nwaitForTestsToComplete(2) count: " + count + "\n");
                try {
                    sleep(1000);
                } catch (InterruptedException ex) {
                    break;
                }
            } else {
                break;
            }
        }

        try {
            sleep(1000);
        } catch (InterruptedException int_ex) {
            System.out.println("Wait after test run was interrupted");
        }
    }
}



