package com.webutils.webserver.kubernetes;

import com.webutils.webserver.requestcontext.WebServerFlavor;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.ListIterator;

public class KubernetesInfo {

    final WebServerFlavor flavor;

    public KubernetesInfo(final WebServerFlavor flavor) {

        this.flavor = flavor;
    }

    public String getKubeInfo() throws IOException {

        // file path to your KubeConfig
        String kubeConfigPath;
        if ((flavor == WebServerFlavor.INTEGRATION_KUBERNETES_TESTS) || (flavor == WebServerFlavor.INTEGRATION_DOCKER_TESTS)) {
            kubeConfigPath = "/usr/src/myapp/config/config";
        } else {
            kubeConfigPath = "/Users/notterness/.kube/config";
        }

        // loading the out-of-cluster config, a kubeconfig from file-system
        //ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build();

        ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build();
        //ApiClient client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);

        CoreV1Api api = new CoreV1Api();
        try {
            V1PodList list =
                    api.listPodForAllNamespaces(null, null, null, null, null,
                            null, null, null, null);
            for (V1Pod item : list.getItems()) {
                String podName = item.getMetadata().getName();
                if (podName.contains("webutils-site")) {
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
            }
        } catch (ApiException api_ex) {
            System.out.println("Kubernetes V1 API exception: " + api_ex.getMessage());
        }

        try {
            V1EndpointsList endpoints = api.listEndpointsForAllNamespaces(true, null, null, null,
                    20, null, null, 50, false);

            for (V1Endpoints endpoint : endpoints.getItems()) {
                if (endpoint.getMetadata().getName().contains("webutils")) {
                    System.out.println("ENDPOINT:  " + endpoint.getMetadata().getName());
                }
            }

        } catch (ApiException api_ex) {
            System.out.println("Kubernetes V1 API exception: " + api_ex.getMessage());
        }

        String podIp = null;

        try {
            V1ServiceList services = api.listServiceForAllNamespaces(true, null, null, null,
                    5, null, null, 100, false);

            for (V1Service service : services.getItems()) {
                if (service.getMetadata().getName().contains("webutils")) {
                    System.out.println("SERVICE:  " + service.getMetadata().getName());

                    V1ServiceSpec spec = service.getSpec();

                    podIp = spec.getClusterIP();
                    System.out.println("  spec - clusterIP: " + spec.getClusterIP());

                    List<V1ServicePort> ports = spec.getPorts();
                    ListIterator<V1ServicePort> portIter = ports.listIterator();
                    while (portIter.hasNext()) {
                        V1ServicePort port = portIter.next();
                        System.out.println("    name: " + port.getName() + " port: " + port.getPort() + " protocol: " + port.getProtocol() +
                                " targetPort: " + port.getTargetPort());
                    }
                }
            }

        } catch (ApiException api_ex) {
            System.out.println("Kubernetes V1 API exception: " + api_ex.getMessage());
        }

        return podIp;
    }

    /*
    public void WatchExample() throws IOException, ApiException
    {
        ApiClient client = Config.defaultClient();
        // infinite timeout
        OkHttpClient httpClient = client.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build();
        client.setHttpClient(httpClient);
        Configuration.setDefaultApiClient(client);

        CoreV1Api api = new CoreV1Api();

        Watch<V1Namespace> watch =
                Watch.createWatch(
                        client,
                        api.listNamespaceCall(null, null, null, null, null, 5, null, null, Boolean.TRUE, null),
                        new TypeToken<Watch.Response<V1Namespace>>() {
                        }.getType());

        try {
            for (Watch.Response<V1Namespace> item : watch) {
                System.out.printf("%s : %s%n", item.type, item.object.getMetadata().getName());
            }
        } finally {
            watch.close();
        }
    }
    */
}
