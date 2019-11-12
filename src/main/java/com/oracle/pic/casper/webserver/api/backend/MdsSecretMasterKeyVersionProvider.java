package com.oracle.pic.casper.webserver.api.backend;

import com.oracle.pic.casper.common.encryption.store.MasterKeyVersionProviders.MasterKeyVersionProvider;
import com.oracle.pic.casper.common.encryption.store.Secrets;
import com.oracle.pic.casper.common.util.CachingSupplier;
import com.oracle.pic.casper.mds.loadbalanced.MdsExecutor;
import com.oracle.pic.casper.mds.operator.GetSecretVersionRequest;
import com.oracle.pic.casper.mds.operator.OperatorMetadataServiceGrpc.OperatorMetadataServiceBlockingStub;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class MdsSecretMasterKeyVersionProvider implements MasterKeyVersionProvider {

    private static final Duration DEFAULT_CACHE_DURATION = Duration.ofMinutes(5);
    private final MdsExecutor<OperatorMetadataServiceBlockingStub> client;
    private final Supplier<String> versionSupplier;

    public MdsSecretMasterKeyVersionProvider(
            MdsExecutor<OperatorMetadataServiceBlockingStub> client,
            Duration requestDeadline) {
        this.client = client;
        this.versionSupplier =
                CachingSupplier.cacheWithExpiration(() -> MdsMetrics.executeWithMetrics(MdsMetrics.OPERATOR_MDS_BUNDLE,
                        MdsMetrics.OPERATOR_GET_SECRET,
                        false,
                        () -> String.valueOf(this.client.execute(c -> c.withDeadlineAfter(requestDeadline.toMillis(),
                                TimeUnit.MILLISECONDS)
                                .getSecretVersion(GetSecretVersionRequest.newBuilder()
                                        .setLabel(Secrets.ENCRYPTION_MASTER_KEY)
                                        .build())).getVersion())), DEFAULT_CACHE_DURATION);
    }

    @Override
    public String get() {
        return versionSupplier.get();
    }
}
