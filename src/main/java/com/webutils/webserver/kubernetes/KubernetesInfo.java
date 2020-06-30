package com.webutils.webserver.kubernetes;

import com.webutils.webserver.requestcontext.WebServerFlavor;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class KubernetesInfo {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesInfo.class);

    private static final int MAX_K8_RETRIES = 10;
    private static final int MAX_TEST_RETRIES = 3;

    final WebServerFlavor flavor;

    public KubernetesInfo(final WebServerFlavor flavor) {

        LOG.info("KubernetesInfo() WebServerFlavor: " + flavor.toString());
        this.flavor = flavor;
    }

    /*
    ** This waits for internal IP address to be valid (or times out)
     */
    public String waitForInternalK8Ip() {
        /*
         ** This simply waits until the IP address can be obtained to insure that the POD is up and running.
         **
         ** NOTE: There must be a better way to determine if the POD has been started...
         */
        String k8IpAddr = null;

        int retryCount = 0;
        int maxRetryCount;
        if (flavor == WebServerFlavor.KUBERNETES_OBJECT_SERVER_TEST) {
            maxRetryCount = MAX_K8_RETRIES;
        } else {
            maxRetryCount = MAX_TEST_RETRIES;
        }

        while ((k8IpAddr == null) && (retryCount < maxRetryCount)) {
            try {
                k8IpAddr = getInternalK8Ip();
            } catch (IOException io_ex) {
                System.out.println("IOException: " + io_ex.getMessage());
                LOG.error("waitForInternalK8Ip() - IOException: " + io_ex.getMessage());
                k8IpAddr = null;
            }

            if (k8IpAddr == null) {
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException intEx) {
                    LOG.error("Trying to obtain internal Kubernetes IP " + intEx.getMessage());
                    break;
                }

                retryCount++;
            }
        }

        return k8IpAddr;
    }

    /*
     ** This waits for external IP address to be valid (or times out)
     */
    public String waitForExternalK8Ip() {
        /*
         ** This simply waits until the IP address can be obtained to insure that the POD is up and running.
         **
         ** NOTE: There must be a better way to determine if the POD has been started...
         */
        String k8IpAddr = null;

        int retryCount = 0;
        int maxRetryCount;
        if (flavor == WebServerFlavor.KUBERNETES_OBJECT_SERVER_TEST) {
            maxRetryCount = MAX_K8_RETRIES;
        } else {
            maxRetryCount = MAX_TEST_RETRIES;
        }

        while ((k8IpAddr == null) && (retryCount < maxRetryCount)) {
            try {
                k8IpAddr = getExternalK8Ip();
            } catch (IOException io_ex) {
                System.out.println("IOException: " + io_ex.getMessage());
                LOG.error("waitForExternalK8Ip() - IOException: " + io_ex.getMessage());
                k8IpAddr = null;
            }

            if (k8IpAddr == null) {
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException intEx) {
                    LOG.error("Trying to obtain external Kubernetes IP " + intEx.getMessage());
                    break;
                }

                retryCount++;
            }
        }

        return k8IpAddr;
    }

    public void getK8Info() throws IOException {
        /*
         ** File path to the KubeConfig - for images running within a Docker container, there must be a mapping between
         **    /usr/src/myapp/config/config to /Users/notterness/.kube/config
         **
         ** For the Kubernetes images this is setup in the deployment-webutils.yaml file.
         ** For Docker runs, it is in the command line to run the Docker container.
         */
        String kubeConfigPath = getKubeConfigPath();

        // loading the out-of-cluster config, a kubeconfig from file-system
        ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build();
        CoreV1Api api = new CoreV1Api(client);


        try {
            /*
             ** The labels can be found by the following (from the V1Pod below):
             **    Map<String, String> labels = item.getMetadata().getLabels();
             ** Use the labelSelector to filter out only the Pod that has app set to webutils-site.
             */
            String labelSelector = "app=webutils-site";
            V1PodList list =
                    api.listPodForAllNamespaces(null, null, null, labelSelector, 5,
                            null, null, 100, null);
            for (V1Pod item : list.getItems()) {
                String podName = item.getMetadata().getName();
                if (podName != null) {

                    System.out.println("POD: " + podName);

                    List<V1Container> containers = item.getSpec().getContainers();
                    for (V1Container container : containers) {
                        System.out.println("  Container - " + container.getName() + " - " + container.getImage());

                        List<V1VolumeMount> volumes = container.getVolumeMounts();
                        for (V1VolumeMount volume : volumes) {
                            System.out.println("   volume mount: " + volume.getName() + " " + volume.getMountPath());
                        }
                    }

                    /*
                     ** The following lists all the volumes associated with a particular POD. There is probably a
                     **   way to determine the volume type (i.e. "emptyDir" or "hostPath") directly without
                     **   checking if the query returns "null".
                     */
                    List<V1Volume> podVolumes = item.getSpec().getVolumes();
                    for (V1Volume volume : podVolumes) {
                        V1EmptyDirVolumeSource emptyDir = volume.getEmptyDir();
                        V1HostPathVolumeSource hostPath = volume.getHostPath();

                        if (emptyDir != null) {
                            System.out.println("emptyDir - " + volume.getName() +
                                    " -- " + emptyDir.getMedium() + " " + volume.getClass().toString());

                        } else if (hostPath != null) {
                            System.out.println("HostPath - " + volume.getName() + " -- " +
                                    hostPath.getPath() + " " + hostPath.getType() + " : " + volume.getClass().toString());
                        } else {
                            System.out.println(volume.getName() + ": " + volume.getClass().toString());
                        }
                    }
                }
            }
        } catch (ApiException api_ex) {
            System.out.println("getK8Info(1) - V1 API exception: " + api_ex.getMessage());
            LOG.error("getK8Info(1) - V1 API exception: " + api_ex.getMessage());
        }

        try {
            V1EndpointsList endpoints = api.listEndpointsForAllNamespaces(true, null, null, null,
                    20, null, null, 50, false);

            for (V1Endpoints endpoint : endpoints.getItems()) {
                if (endpoint.getMetadata().getName().contains("webutils")) {
                    System.out.println("ENDPOINT:  " + endpoint.getMetadata().getName());

                    /*
                     ** Just to display information
                     */
                    List<V1EndpointSubset> subsets = endpoint.getSubsets();
                    for (V1EndpointSubset subset : subsets) {
                        List<V1EndpointAddress> endpointAddrList = subset.getAddresses();

                        for (V1EndpointAddress addr : endpointAddrList) {
                            System.out.println("  subset V1EndpointAddr ip: " + addr.getIp());
                            LOG.info("  subset V1EndpointAddr ip: " + addr.getIp());
                        }
                    }

                }
            }
        } catch (ApiException api_ex) {
            System.out.println("getK8Info(2) - V1 API exception: " + api_ex.getMessage());
            LOG.error("getK8Info(2) - V1 API exception: " + api_ex.getMessage());
        }
    }

    /*
    ** This obtains the external IP (how services or programs running outside of the POD can access the services
    **   running within the POD) of the webutils-site POD. THis is dependent upon the type of service defined
    **   in the service-webutils.yaml file. Currently it is a fairly basic service and the IP address that is
    **   exposed is defined by the "LoadBalancer Ingress" field shown by running the "kubectl describe service"
    **   command.
     */
    public String getExternalK8Ip() throws IOException {

        /*
        ** File path to the KubeConfig - for images running within a Docker container, there must be a mapping between
        **    /usr/src/myapp/config/config to /Users/notterness/.kube/config
        **
        ** For the Kubernetes images this is setup in the deployment-webutils.yaml file.
        ** For Docker runs, it is in the command line to run the Docker container.
         */
        String kubeConfigPath = getKubeConfigPath();

        // loading the out-of-cluster config, a kubeconfig from file-system
        ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build();
        CoreV1Api api = new CoreV1Api(client);

        /*
         ** The following block of code obtains a list of services being run by Kubernetes. This is pretty simple in that
         **   it doesn't perform any filtering for the results. It does limit the number of "services" being returned
         **   to 5.
         */
        String externalPodIp = null;

        try {
            /*
             ** Use the fieldSelector to only search for services associated with webutils-site. There should only
             **   be a single service that matches the search, but leaving the limit at 5 just to be sure.
             */
            String fieldSelector = "metadata.name=webutils-service";
            V1ServiceList services = api.listServiceForAllNamespaces(false, null, fieldSelector, null,
                    5, null, null, 100, false);

            for (V1Service service : services.getItems()) {
                System.out.println("SERVICE:  " + service.getMetadata().getName());

                V1ServiceStatus serviceStatus = service.getStatus();
                V1LoadBalancerStatus balancerStatus = serviceStatus.getLoadBalancer();
                System.out.println("LoadBalancer: " + balancerStatus.toString());

                List<V1LoadBalancerIngress> ingressList = balancerStatus.getIngress();
                if (ingressList != null) {
                    /*
                    ** Only going to pull the first IP address from the ingress list no matter how matter are present.
                    **   To fix this would require a change to the database table to allow for multiple ingress
                    **   IP addresses.
                     */
                    ListIterator<V1LoadBalancerIngress> iter = ingressList.listIterator();
                    if (iter.hasNext()) {
                        V1LoadBalancerIngress ingress = iter.next();
                        externalPodIp = ingress.getIp();
                        if (externalPodIp == null) {
                            externalPodIp = ingress.getHostname();
                        }
                    }
                }
                System.out.println("  LoadBalancer Ingress IP: " + externalPodIp);
            }
        } catch (ApiException api_ex) {
            System.out.println("getExternalKubeIp() - V1 API exception: " + api_ex.getMessage());
            LOG.error("getExternalKubeIp() - V1 API exception: " + api_ex.getMessage());
        }

        return externalPodIp;
    }

    /*
    ** This will not return a valid IP until after the service has started. The service will take time to start so, the
    **   caller of this will need to sleep and retry if null is rreturned.
     */
    public String getInternalK8Ip() throws IOException {

        // file path to your KubeConfig
        String kubeConfigPath = getKubeConfigPath();

        // loading the out-of-cluster config, a kubeconfig from file-system
        ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build();
        CoreV1Api api = new CoreV1Api(client);

        String internalPodIp = null;

        /*
        ** Use the fieldSelector to only search for services associated with webutils-site. There should only
        **   be a single service that matches the search, so the limit is set at 1 just to be sure.
         */
        String fieldSelector = "metadata.name=webutils-service";

        try {
            V1EndpointsList endpoints = api.listEndpointsForAllNamespaces(true, null, fieldSelector, null,
                    1, null, null, 50, false);

            for (V1Endpoints endpoint : endpoints.getItems()) {
                System.out.println("ENDPOINT:  " + endpoint.getMetadata().getName());
                LOG.info("ENDPOINT:  " + endpoint.getMetadata().getName() );

                List<V1EndpointSubset> subsets = endpoint.getSubsets();
                for (V1EndpointSubset subset : subsets) {
                    List<V1EndpointAddress> endpointAddrList = subset.getAddresses();

                    for (V1EndpointAddress addr : endpointAddrList) {
                        System.out.println("  subset V1EndpointAddr ip: " + addr.getIp());
                        System.out.println("  LoadBalancer Ingress ip: ");
                        LOG.info("  subset V1EndpointAddr ip: " + addr.getIp());

                        internalPodIp = addr.getIp();
                        break;
                    }

                    if (internalPodIp != null) {
                        break;
                    }
                }
            }
        } catch (ApiException api_ex) {
            System.out.println("getInternalKubeIp() - V1 API exception: listEndpointsForAllNamespaces - " + api_ex.getMessage());
            LOG.error("getInternalKubeIp() - V1 API exception: listEndpointsForAllNamespaces - " + api_ex.getMessage());
        }

        return internalPodIp;
    }

    /*
     ** This returns a Map<Storage Server Name or Object Server Name:String, NodePort:Integer> of all of the Object and
     **   Storage Servers found in the webutils-service.
     **
     ** NOTE: Port is what is used to communicate with the Docker Image from inside the POD.
     */
    public int getServicePorts(Map<String, Integer> serversInfo) throws IOException {
        /*
         ** File path to the KubeConfig - for images running within a Docker container, there must be a mapping between
         **    /usr/src/myapp/config/config to /Users/notterness/.kube/config
         **
         ** For the Kubernetes images this is setup in the deployment-webutils.yaml file.
         ** For Docker runs, it is in the command line to run the Docker container.
         */
        String kubeConfigPath = getKubeConfigPath();

        ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build();

        int servicePortCount = 0;
        CoreV1Api api = new CoreV1Api(client);
        try {
            /*
             ** Use the fieldSelector to only search for services associated with webutils-site. There should only
             **   be a single service that matches the search, but leaving the limit at 5 just to be sure.
             */
            String fieldSelector = "metadata.name=webutils-service";
            V1ServiceList services = api.listServiceForAllNamespaces(true, null, fieldSelector, null,
                    5, null, null, 100, false);

            for (V1Service service : services.getItems()) {
                System.out.println("SERVICE:  " + service.getMetadata().getName());

                V1ServiceSpec spec = service.getSpec();

                System.out.println("  spec - clusterIP: " + spec.getClusterIP());

                List<V1ServicePort> ports = spec.getPorts();
                for (V1ServicePort port : ports) {
                    System.out.println("    name: " + port.getName() + " port: " + port.getPort() + " protocol: " + port.getProtocol() +
                            " targetPort: " + port.getTargetPort() + " NodePort: " + port.getNodePort());

                    serversInfo.put(port.getName(), port.getPort());
                    servicePortCount++;
                }
            }
        } catch (ApiException api_ex) {
            System.out.println("getStorageServerNodePorts() - V1 API exception: " + api_ex.getMessage());
            LOG.error("getStorageServerNodePorts() - V1 API exception: " + api_ex.getMessage());
        }

        return servicePortCount;
    }

    /*
     ** This returns a Map<Storage Server Name or Object Server Name:String, NodePort:Integer> of all of the Object and
     **   Storage Servers found in the webutils-service.
     **
     ** NOTE: NodePort is what is used to communicate with the Docker Image from outside the POD.
     */
    public int getServiceNodePorts(Map<String, Integer> serversInfo) throws IOException {
        /*
         ** File path to the KubeConfig - for images running within a Docker container, there must be a mapping between
         **    /usr/src/myapp/config/config to /Users/notterness/.kube/config
         **
         ** For the Kubernetes images this is setup in the deployment-webutils.yaml file.
         ** For Docker runs, it is in the command line to run the Docker container.
         */
        String kubeConfigPath = getKubeConfigPath();

        ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build();

        int servicePortCount = 0;
        CoreV1Api api = new CoreV1Api(client);
        try {
            /*
             ** Use the fieldSelector to only search for services associated with webutils-site. There should only
             **   be a single service that matches the search, but leaving the limit at 5 just to be sure.
             */
            String fieldSelector = "metadata.name=webutils-service";
            V1ServiceList services = api.listServiceForAllNamespaces(true, null, fieldSelector, null,
                    5, null, null, 100, false);

            for (V1Service service : services.getItems()) {
                System.out.println("SERVICE:  " + service.getMetadata().getName());

                V1ServiceSpec spec = service.getSpec();

                System.out.println("  spec - clusterIP: " + spec.getClusterIP());

                List<V1ServicePort> ports = spec.getPorts();
                for (V1ServicePort port : ports) {
                    System.out.println("    name: " + port.getName() + " port: " + port.getPort() + " protocol: " + port.getProtocol() +
                            " targetPort: " + port.getTargetPort() + " NodePort: " + port.getNodePort());

                    serversInfo.put(port.getName(), port.getNodePort());
                    servicePortCount++;
                }
            }
        } catch (ApiException api_ex) {
            System.out.println("getStorageServerNodePorts() - V1 API exception: " + api_ex.getMessage());
            LOG.error("getStorageServerNodePorts() - V1 API exception: " + api_ex.getMessage());
        }

        return servicePortCount;
    }

    /*
     ** This returns a Map<Storage Server Name:String, TargetPort:Integer> of all of the Storage Servers found in
     **   the webutils-service.
     ** It checks to make sure that the name of the Service ports contains "storage-server". This is done to filter out
     **   the "object-server" ports.
     **
     ** NOTE: TargetPort is what is used to communicate with the Docker Image from within the POD.
     */
    public int getStorageServerPorts(Map<String, Integer> storageServersInfo) throws IOException {
        /*
         ** File path to the KubeConfig - for images running within a Docker container, there must be a mapping between
         **    /usr/src/myapp/config/config to /Users/notterness/.kube/config
         **
         ** For the Kubernetes images this is setup in the deployment-webutils.yaml file.
         ** For Docker runs, it is in the command line to run the Docker container.
         */
        String kubeConfigPath = getKubeConfigPath();

        ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build();

        int storageServerCount = 0;
        CoreV1Api api = new CoreV1Api(client);
        try {
            /*
             ** Use the fieldSelector to only search for services associated with webutils-site. There should only
             **   be a single service that matches the search, but leaving the limit at 5 just to be sure.
             */
            String fieldSelector = "metadata.name=webutils-service";
            V1ServiceList services = api.listServiceForAllNamespaces(true, null, fieldSelector, null,
                    5, null, null, 100, false);

            for (V1Service service : services.getItems()) {
                System.out.println("SERVICE:  " + service.getMetadata().getName());

                V1ServiceSpec spec = service.getSpec();

                System.out.println("  spec - clusterIP: " + spec.getClusterIP());

                List<V1ServicePort> ports = spec.getPorts();

                for (V1ServicePort port : ports) {
                    if (port.getName().contains("storage-server")) {
                        System.out.println("    name: " + port.getName() + " port: " + port.getPort() + " protocol: " + port.getProtocol() +
                                " targetPort: " + port.getTargetPort() + " NodePort: " + port.getNodePort());

                        storageServersInfo.put(port.getName(), port.getPort());
                        storageServerCount++;
                    }
                }
            }
        } catch (ApiException api_ex) {
            System.out.println("getStorageServerNodePorts() - V1 API exception: " + api_ex.getMessage());
            LOG.error("getStorageServerNodePorts() - V1 API exception: " + api_ex.getMessage());
        }

        return storageServerCount;
    }

    /*
    ** This is used to pick the proper path to the .kube/config file. For The Docker images and running within
    **   the Kubernetes POD, the /usr/src/myapp/config directory is mapped to the local file system directory
    **   as part of either the Docker run command line:
    **
    **   docker run --name ClientTest -v /Users/notterness/WebServer/webserver/logs:/usr/src/myapp/logs -v /Users/notterness/.kube:/usr/src/myapp/config -it clienttest:1
    **
    **  Or in the deployment yaml file:
    **
    **    volumeMounts:
    **      -
    **        name: kube-config-volume
    **        mountPath: /usr/src/myapp/config
    **
    **  --> And in the volumes: section
    **
    **      -
    **        name: kube-config-volume
    **        hostPath:
    **          path: /Users/notterness/.kube
     */
    private String getKubeConfigPath() {
        String kubeConfigPath;

        if ((flavor == WebServerFlavor.INTEGRATION_KUBERNETES_TESTS) ||
                (flavor == WebServerFlavor.INTEGRATION_DOCKER_TESTS) ||
                (flavor == WebServerFlavor.KUBERNETES_OBJECT_SERVER_TEST)) {
            kubeConfigPath = "/usr/src/myapp/config/config";
        } else {
            kubeConfigPath = "/Users/notterness/.kube/config";
        }

        LOG.info("kubeConfigPath: " + kubeConfigPath);

        return kubeConfigPath;
    }
}
