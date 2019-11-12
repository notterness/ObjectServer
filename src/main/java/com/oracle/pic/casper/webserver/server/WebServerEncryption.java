package com.oracle.pic.casper.webserver.server;

import com.oracle.pic.casper.common.config.v2.CommonConfigurations;
import com.oracle.pic.casper.common.certs.CertificateStore;
import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.encryption.service.KmsClientMemory;
import com.oracle.pic.casper.common.encryption.service.RemoteKeyManagementService;
import com.oracle.pic.casper.common.encryption.service.SecretStoreKeyManagementService;
import com.oracle.pic.casper.common.encryption.store.MasterKeyVersionProviders;
import com.oracle.pic.casper.common.encryption.store.SecretStore;
import com.oracle.pic.casper.webserver.api.backend.MdsSecretMasterKeyVersionProvider;

/**
 * When the STANDARD flavor is used, the web server will use production encryption:
 *  - For KMS-enabled buckets, it will talk to KMS to generate, encrypt, and decrypt Data Encryption Keys (DEKs).
 *    Each chunk has its own (unique?) DEK that is stored encrypted in object DB.
 *  - For normal buckets, it will use operator-mds to identify the master key version, then it will fetch that key
 *    from the provided {@link SecretStore} and use it to encrypt and decrypt DEKs.  Raw DEKs are generated locally.
 *
 * When the INTEGRATION_TEST flavor is used, the web server will use test encryption:
 *  - For KMS-enabled buckets, it will talk to an in-memory KMS client that vends unique (random) DEKs, but no-ops the
 *    encryption and decryption of them.
 *  - For normal buckets, it will still use the master key in the provided {@link SecretStore} (callers are
 *    responsible for ensuring that the implementation is available during tests), but the master key version is
 *    hardcoded to "latest".  Whatever key the secret store returns for that version is used to encrypt and decrypt
 *    DEKs.  Raw DEKs are still generated locally.
 */
public final class WebServerEncryption {

    private final DecidingKeyManagementService kms;

    public WebServerEncryption(WebServerFlavor flavor,
                               CasperConfig config,
                               SecretStore secretStore,
                               CertificateStore certStore,
                               MdsClients mdsClients) {
        if (flavor == WebServerFlavor.STANDARD) {
            MasterKeyVersionProviders.MasterKeyVersionProvider mkvp = new MdsSecretMasterKeyVersionProvider(
                    mdsClients.getOperatorMdsExecutor(), mdsClients.getOperatorDeadline());
            CommonConfigurations commonConfig = config.getCommonConfigurations();
            kms = new DecidingKeyManagementService(RemoteKeyManagementService.create(
                    commonConfig.getServiceEndpointConfiguration().getIdentityEndpointConfiguration().getStsEndpoint()
                            .getUrl(),
                    commonConfig.getServicePrincipalConfiguration().getTenantId(),
                    commonConfig.getServicePrincipalConfiguration().getCertDir(),
                    config.getRegion(),
                    config.getAvailabilityDomain(),
                    certStore),
                    new SecretStoreKeyManagementService(secretStore, mkvp));
        } else {
            MasterKeyVersionProviders.MasterKeyVersionProvider mkvp = MasterKeyVersionProviders.latest();
            kms = new DecidingKeyManagementService(new RemoteKeyManagementService(new KmsClientMemory()),
                    new SecretStoreKeyManagementService(secretStore, mkvp));
        }
    }

    public DecidingKeyManagementService getDecidingManagementServiceProvider() {
        return kms;
    }
}
