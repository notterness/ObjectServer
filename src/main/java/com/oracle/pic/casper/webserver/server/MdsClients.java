package com.oracle.pic.casper.webserver.server;

import com.google.common.collect.ImmutableSet;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.mds.client.InProcessObjectMdsClientFactory;
import com.oracle.pic.casper.mds.client.InProcessOperatorMdsClientFactory;
import com.oracle.pic.casper.mds.client.InProcessTenantMdsClientFactory;
import com.oracle.pic.casper.mds.client.InProcessWorkrequestMdsClientFactory;
import com.oracle.pic.casper.mds.client.ObjectMdsClientFactory;
import com.oracle.pic.casper.mds.client.OperatorMdsClientFactory;
import com.oracle.pic.casper.mds.client.TenantMdsClientFactory;
import com.oracle.pic.casper.mds.client.WorkrequestMdsClientFactory;
import com.oracle.pic.casper.mds.loadbalanced.MdsClientSideLoadbalancer;
import com.oracle.pic.casper.mds.loadbalanced.MdsExecutor;
import com.oracle.pic.casper.mds.object.ObjectConstants;
import com.oracle.pic.casper.mds.object.ObjectMdsTestUtils;
import com.oracle.pic.casper.mds.object.ObjectServiceGrpc.ObjectServiceBlockingStub;
import com.oracle.pic.casper.mds.operator.OperatorConstants;
import com.oracle.pic.casper.mds.operator.OperatorMdsTestUtils;
import com.oracle.pic.casper.mds.operator.OperatorMetadataServiceGrpc.OperatorMetadataServiceBlockingStub;
import com.oracle.pic.casper.mds.resolver.MdsDnsResolver;
import com.oracle.pic.casper.mds.resolver.MdsStaticResolver;
import com.oracle.pic.casper.mds.tenant.TenantConstants;
import com.oracle.pic.casper.mds.tenant.TenantMdsTestUtils;
import com.oracle.pic.casper.mds.tenant.TenantMetadataServiceGrpc.TenantMetadataServiceBlockingStub;
import com.oracle.pic.casper.mds.workrequest.WorkRequestConstants;
import com.oracle.pic.casper.mds.workrequest.WorkRequestMdsTestUtils;
import com.oracle.pic.casper.mds.workrequest.WorkRequestMetadataServiceGrpc.WorkRequestMetadataServiceBlockingStub;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.time.Duration;
import javax.annotation.Nonnull;

/**
 * The server is set to an in-memory version when the WebServerFlavor is INTEGRATION_TESTS.
 * TODO: Use a separate in-memory MDS server for each instantiation of MdsCients.
 */
public final class MdsClients {

    private final Duration objectRequestDeadline;
    private final Duration listObjectRequestDeadline;
    private final Duration tenantRequestDeadline;
    private final Duration operatorDeadline;
    private final Duration workRequestDeadline;

    private final MdsClientSideLoadbalancer<ObjectServiceBlockingStub> objectLb;
    private final MdsExecutor<ObjectServiceBlockingStub> objectMdsExecutor;
    private final int listObjectsMaxMessageBytes;
    private final int getObjectMaxMessageBytes;

    private final MdsClientSideLoadbalancer<TenantMetadataServiceBlockingStub> tenantLb;
    private final MdsExecutor<TenantMetadataServiceBlockingStub> tenantMdsExecutor;

    private final MdsClientSideLoadbalancer<WorkRequestMetadataServiceBlockingStub> workrequestLb;
    private final MdsExecutor<WorkRequestMetadataServiceBlockingStub> workrequestMdsExecutor;

    private final MdsClientSideLoadbalancer<OperatorMetadataServiceBlockingStub> operatorLb;
    private final MdsExecutor<OperatorMetadataServiceBlockingStub> operatorMdsExecutor;

    private final WebServerFlavor flavor;
    private final ConfigRegion configRegion;

    private boolean useInProcess() {
        return flavor == WebServerFlavor.INTEGRATION_TESTS || configRegion.equals(ConfigRegion.LOCAL);
    }

    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    public MdsClients(WebServerFlavor flavor, CasperConfig config) {
        this.flavor = flavor;
        this.configRegion = config.getRegion();

        final MdsEndpoint objectEndpoint =
                new MdsEndpoint(config.getMdsClientConfiguration().getObjectMdsEndpoint());
        final MdsEndpoint tenantEndpoint =
                new MdsEndpoint(config.getMdsClientConfiguration().getTenantMdsEndpoint());
        final MdsEndpoint operatorEndpoint =
                new MdsEndpoint(config.getMdsClientConfiguration().getOperatorMdsEndpoint());
        final MdsEndpoint workrequestEndpoint =
                new MdsEndpoint(config.getMdsClientConfiguration().getWorkRequestMdsEndpoint());

        final String mtlsBasePath =
                config.getCommonConfigurations().getMtlsClientConfiguration().getClientCertFolder();

        if (!useInProcess()) {
            objectLb = new MdsClientSideLoadbalancer<>(
                    config.getMdsClientConfiguration().getObjectLbConfig(),
                    new ObjectMdsClientFactory(objectEndpoint.getUrl(), mtlsBasePath),
                    new MdsDnsResolver(objectEndpoint.getUrl(), objectEndpoint.getPort()),
                    ObjectConstants.SERVICE_NAME);

            tenantLb = new MdsClientSideLoadbalancer<>(
                    config.getMdsClientConfiguration().getTenantLbConfig(),
                    new TenantMdsClientFactory(tenantEndpoint.getUrl(), mtlsBasePath),
                    new MdsDnsResolver(tenantEndpoint.getUrl(), tenantEndpoint.getPort()),
                    TenantConstants.SERVICE_NAME);

            operatorLb = new MdsClientSideLoadbalancer<>(
                    config.getMdsClientConfiguration().getOperatorLbConfig(),
                    new OperatorMdsClientFactory(operatorEndpoint.getUrl(), mtlsBasePath),
                    new MdsDnsResolver(operatorEndpoint.getUrl(), operatorEndpoint.getPort()),
                    OperatorConstants.SERVICE_NAME);

            workrequestLb = new MdsClientSideLoadbalancer<>(
                    config.getMdsClientConfiguration().getWorkrequestLbConfig(),
                    new WorkrequestMdsClientFactory(workrequestEndpoint.getUrl(), mtlsBasePath),
                    new MdsDnsResolver(workrequestEndpoint.getUrl(), workrequestEndpoint.getPort()),
                    WorkRequestConstants.SERVICE_NAME);
        } else {
            objectLb = new MdsClientSideLoadbalancer<>(
                    config.getMdsClientConfiguration().getObjectLbConfig(),
                    new InProcessObjectMdsClientFactory(),
                    new MdsStaticResolver(ImmutableSet.of("inProcess")),
                    ObjectConstants.SERVICE_NAME);
            tenantLb = new MdsClientSideLoadbalancer<>(
                    config.getMdsClientConfiguration().getTenantLbConfig(),
                    new InProcessTenantMdsClientFactory(),
                    new MdsStaticResolver(ImmutableSet.of("inProcess")),
                    TenantConstants.SERVICE_NAME);
            operatorLb = new MdsClientSideLoadbalancer<>(
                    config.getMdsClientConfiguration().getOperatorLbConfig(),
                    new InProcessOperatorMdsClientFactory(),
                    new MdsStaticResolver(ImmutableSet.of("inProcess")),
                    OperatorConstants.SERVICE_NAME);
            workrequestLb = new MdsClientSideLoadbalancer<>(
                    config.getMdsClientConfiguration().getWorkrequestLbConfig(),
                    new InProcessWorkrequestMdsClientFactory(),
                    new MdsStaticResolver(ImmutableSet.of("inProcess")),
                    WorkRequestConstants.SERVICE_NAME);
        }

        objectMdsExecutor = new MdsExecutor<>(objectLb);
        tenantMdsExecutor = new MdsExecutor<>(tenantLb);
        operatorMdsExecutor = new MdsExecutor<>(operatorLb);
        workrequestMdsExecutor = new MdsExecutor<>(workrequestLb);

        objectRequestDeadline = config.getMdsClientConfiguration().getObjectMdsRequestDeadline();
        listObjectRequestDeadline = config.getMdsClientConfiguration().getListObjectMdsRequestDeadline();
        listObjectsMaxMessageBytes = config.getMdsClientConfiguration().getListObjectsMaxMessageBytes();
        getObjectMaxMessageBytes = config.getMdsClientConfiguration().getGetObjectMaxMessageBytes();
        tenantRequestDeadline = config.getMdsClientConfiguration().getTenantMdsRequestDeadline();
        operatorDeadline = config.getMdsClientConfiguration().getOperatorMdsRequestDeadline();
        workRequestDeadline = config.getMdsClientConfiguration().getWorkRequestMdsRequestDeadline();
    }

    public void start() {
        if (useInProcess()) {
            ObjectMdsTestUtils.resetObjectMDS();
            TenantMdsTestUtils.resetTenantMDS();
            OperatorMdsTestUtils.resetOperatorMDS();
            WorkRequestMdsTestUtils.resetWorkRequestMDS();
        }

        objectLb.start();
        tenantLb.start();
        operatorLb.start();
        workrequestLb.start();

    }

    public void stop() {
        objectLb.stop();
        tenantLb.stop();
        operatorLb.stop();
        workrequestLb.stop();

        if (useInProcess()) {
            ObjectMdsTestUtils.teardownObjectMDS();
            TenantMdsTestUtils.teardownTenantMDS();
            OperatorMdsTestUtils.teardownOperatorMDS();
            WorkRequestMdsTestUtils.teardownWorkRequestMDS();
        }
    }

    public Duration getObjectRequestDeadline() {
        return objectRequestDeadline;
    }

    public Duration getListObjectRequestDeadline() {
        return listObjectRequestDeadline;
    }

    public int getListObjectsMaxMessageBytes() {
        return listObjectsMaxMessageBytes;
    }

    public int getGetObjectMaxMessageBytes() {
        return getObjectMaxMessageBytes;
    }

    public Duration getTenantRequestDeadline() {
        return tenantRequestDeadline;
    }

    public Duration getOperatorDeadline() {
        return operatorDeadline;
    }

    public Duration getWorkRequestDeadline() {
        return workRequestDeadline;
    }

    public MdsExecutor<ObjectServiceBlockingStub> getObjectMdsExecutor() {
        return objectMdsExecutor;
    }

    public MdsExecutor<TenantMetadataServiceBlockingStub> getTenantMdsExecutor() {
        return tenantMdsExecutor;
    }

    public MdsExecutor<WorkRequestMetadataServiceBlockingStub> getWorkrequestMdsExecutor() {
        return workrequestMdsExecutor;
    }

    public MdsExecutor<OperatorMetadataServiceBlockingStub> getOperatorMdsExecutor() {
        return operatorMdsExecutor;
    }

    private static class MdsEndpoint {
        private String url;
        private String port;

        MdsEndpoint(@Nonnull String endpoint) {
            final String[] tokens = endpoint.split(":");

            url = tokens[0];
            port = tokens[1];
        }

        public String getUrl() {
            return url;
        }

        public String getPort() {
            return port;
        }
    }
}
