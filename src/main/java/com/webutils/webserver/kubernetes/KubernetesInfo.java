package com.webutils.webserver.kubernetes;

import com.webutils.webserver.requestcontext.WebServerFlavor;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
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
import java.util.Objects;

public class KubernetesInfo {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesInfo.class);

    final WebServerFlavor flavor;

    public KubernetesInfo(final WebServerFlavor flavor) {

        LOG.info("KubernetesInfo() WebServerFlavor: " + flavor.toString());
        this.flavor = flavor;
    }

    public String getExternalKubeIp() throws IOException {

        /*
        ** File path to the KubeConfig - for images running within a Docker container, there must be a mapping between
        **    /usr/src/myapp/config/config to /Users/notterness/.kube/config
        **
        ** For the Kubernetes images this is setup in the deployment-webutils.yaml file.
        ** For Docker runs, it is in the command line to run the Docker container.
         */
        String kubeConfigPath = getKubeConfigPath();

        // loading the out-of-cluster config, a kubeconfig from file-system
        //ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build();

        ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build();
        //ApiClient client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);

        CoreV1Api api = new CoreV1Api();
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

                System.out.println("POD: " + podName);

                List<V1Container> containers = item.getSpec().getContainers();
                ListIterator<V1Container> iter = containers.listIterator();
                while (iter.hasNext()) {
                    V1Container container = iter.next();

                    System.out.println("  Container - " + container.getName() + " - " + container.getImage());

                    List<V1VolumeMount> volumes = container.getVolumeMounts();
                    ListIterator<V1VolumeMount> contVolIter = volumes.listIterator();
                    while (contVolIter.hasNext()) {
                        V1VolumeMount volume = contVolIter.next();

                        System.out.println("   volume mount: " + volume.getName() + " " + volume.getMountPath());
                    }

                }

                /*
                 ** The following lists all the volumes associated with a particular POD. There is probably a
                 **   way to determine the volume type (i.e. "emptyDir" or "hostPath") directly without
                 **   checking if the query returns "null".
                 */
                List<V1Volume> podVolumes = item.getSpec().getVolumes();
                ListIterator<V1Volume> podVolIter = podVolumes.listIterator();
                while (podVolIter.hasNext()) {
                    V1Volume volume = podVolIter.next();

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
        } catch (ApiException api_ex) {
            System.out.println("getExternalKubeIp(1) - V1 API exception: " + api_ex.getMessage());
            LOG.error("getExternalKubeIp(1) - V1 API exception: " + api_ex.getMessage());
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
                    ListIterator<V1EndpointSubset> subsetIter = subsets.listIterator();
                    while (subsetIter.hasNext()) {
                        V1EndpointSubset subset = subsetIter.next();

                        List<V1EndpointAddress> endpointAddrList = subset.getAddresses();
                        ListIterator<V1EndpointAddress> endpointAddrIter = endpointAddrList.listIterator();
                        while (endpointAddrIter.hasNext()) {
                            V1EndpointAddress addr = endpointAddrIter.next();
                            System.out.println("  subset V1EndpointAddr ip: " + addr.getIp());
                            LOG.info("  subset V1EndpointAddr ip: " + addr.getIp());
                        }
                    }

                }
            }

        } catch (ApiException api_ex) {
            System.out.println("getExternalKubeIp(2) - V1 API exception: " + api_ex.getMessage());
            LOG.error("getExternalKubeIp(2) - V1 API exception: " + api_ex.getMessage());
        }


        /*
         ** The following block of code obtains a list of services being run by Kubernetes. This is pretty simple in that
         **   it doesn't perform any filtering for the results. It does limit the m=number of "services" being returned
         **   to 5.
         */
        String externalPodIp = null;

        try {
            /*
             ** Use the fieldSelector to only search for services associated with webutils-site. There should only
             **   be a single service that matches the search, but leaving the limit at 5 just to be sure.
             */
            String fieldSelector = "metadata.name=webutils-site";
            V1ServiceList services = api.listServiceForAllNamespaces(true, null, fieldSelector, null,
                    5, null, null, 100, false);

            for (V1Service service : services.getItems()) {
                System.out.println("SERVICE:  " + service.getMetadata().getName());

                V1ServiceSpec spec = service.getSpec();

                externalPodIp = spec.getClusterIP();
                System.out.println("  spec - clusterIP: " + spec.getClusterIP());

                List<V1ServicePort> ports = spec.getPorts();
                ListIterator<V1ServicePort> portIter = ports.listIterator();
                while (portIter.hasNext()) {
                    V1ServicePort port = portIter.next();
                    System.out.println("    name: " + port.getName() + " port: " + port.getPort() + " protocol: " + port.getProtocol() +
                            " targetPort: " + port.getTargetPort());
                }
            }
        } catch (ApiException api_ex) {
            System.out.println("getExternalKubeIp(3) - V1 API exception: " + api_ex.getMessage());
            LOG.error("getExternalKubeIp(3) - V1 API exception: " + api_ex.getMessage());
        }

        return externalPodIp;
    }

    /*
    ** This will not return a valid IP until after the service has started. The service will take time to start so, the
    **   caller of this will need to sleep and retry if null is rreturned.
     */
    public String getInternalKubeIp() throws IOException {

        // file path to your KubeConfig
        String kubeConfigPath = getKubeConfigPath();

        // loading the out-of-cluster config, a kubeconfig from file-system
        ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build();
        Configuration.setDefaultApiClient(client);

        CoreV1Api api = new CoreV1Api();

        String internalPodIp = null;

        /*
        ** Use the fieldSelector to only search for services associated with webutils-site. There should only
        **   be a single service that matches the search, so the limit is set at 1 just to be sure.
         */
        String fieldSelector = "metadata.name=webutils-site";

        try {
            V1EndpointsList endpoints = api.listEndpointsForAllNamespaces(true, null, fieldSelector, null,
                    1, null, null, 50, false);

            for (V1Endpoints endpoint : endpoints.getItems()) {
                System.out.println("ENDPOINT:  " + endpoint.getMetadata().getName());
                LOG.info("ENDPOINT:  " + endpoint.getMetadata().getName());

                List<V1EndpointSubset> subsets = endpoint.getSubsets();
                ListIterator<V1EndpointSubset> subsetIter = subsets.listIterator();
                while (subsetIter.hasNext()) {
                    V1EndpointSubset subset = subsetIter.next();

                    List<V1EndpointAddress> endpointAddrList = subset.getAddresses();
                    ListIterator<V1EndpointAddress> endpointAddrIter = endpointAddrList.listIterator();
                    while (endpointAddrIter.hasNext()) {
                        V1EndpointAddress addr = endpointAddrIter.next();
                        System.out.println("  subset V1EndpointAddr ip: " + addr.getIp());
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
