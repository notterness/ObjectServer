package com.oracle.pic.casper.webserver.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.pic.casper.common.certs.CertificateStore;
import com.oracle.pic.casper.common.config.AvailabilityDomains;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.config.failsafe.AuthDataPlaneFailSafeConfig;
import com.oracle.pic.casper.common.config.v2.CacheConfiguration;
import com.oracle.pic.casper.common.config.v2.CasperConfig;
import com.oracle.pic.casper.common.config.v2.CommonConfigurations;
import com.oracle.pic.casper.common.config.v2.HttpClientConfiguration;
import com.oracle.pic.casper.common.config.v2.IdentityEndpointConfiguration;
import com.oracle.pic.casper.common.config.v2.ServiceGatewayConfiguration;
import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.common.s2s.ServiceAuthenticationClientFactory;
import com.oracle.pic.casper.webserver.auth.AuthTestConstants;
import com.oracle.pic.casper.webserver.auth.dataplane.AuthDataPlaneClient;
import com.oracle.pic.casper.webserver.auth.dataplane.AuthDataPlaneClientApacheImpl;
import com.oracle.pic.casper.common.auth.dataplane.MetadataClient;
import com.oracle.pic.casper.webserver.auth.dataplane.S3SigningKeyClient;
import com.oracle.pic.casper.webserver.auth.dataplane.S3SigningKeyClientImpl;
import com.oracle.pic.casper.common.auth.dataplane.ServiceAuthenticator;
import com.oracle.pic.casper.common.auth.dataplane.ServiceAuthenticatorImpl;
import com.oracle.pic.casper.webserver.auth.dataplane.SwiftAuthenticator;
import com.oracle.pic.casper.webserver.auth.dataplane.SwiftAuthenticatorImpl;
import com.oracle.pic.casper.webserver.auth.dataplane.SwiftCredentialsClient;
import com.oracle.pic.casper.webserver.auth.dataplane.SwiftCredentialsClientImpl;
import com.oracle.pic.casper.webserver.auth.dataplane.SwiftLegacyAuthenticator;
import com.oracle.pic.casper.webserver.auth.dataplane.SwiftLegacyAuthenticatorImpl;
import com.oracle.pic.casper.webserver.auth.limits.IdentityLimitsClient;
import com.oracle.pic.casper.webserver.auth.limits.InMemoryLimitsClient;
import com.oracle.pic.casper.webserver.auth.limits.Limits;
import com.oracle.pic.casper.webserver.auth.limits.LimitsClient;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.AsyncAuthenticatorImpl;
import com.oracle.pic.casper.webserver.api.auth.Authenticator;
import com.oracle.pic.casper.webserver.api.auth.AuthenticatorImpl;
import com.oracle.pic.casper.webserver.api.auth.AuthenticatorPassThroughImpl;
import com.oracle.pic.casper.webserver.api.auth.Authorizer;
import com.oracle.pic.casper.webserver.api.auth.AuthorizerImpl;
import com.oracle.pic.casper.webserver.api.auth.AuthorizerPassThroughImpl;
import com.oracle.pic.casper.webserver.api.auth.JWTAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledMetadataClient;
import com.oracle.pic.casper.webserver.api.auth.ResourceControlledServicePrincipalClient;
import com.oracle.pic.casper.webserver.api.auth.S3AsyncAuthenticator;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.auth.S3AuthenticatorImpl;
import com.oracle.pic.casper.webserver.api.auth.S3AuthenticatorPassThroughImpl;
import com.oracle.pic.casper.webserver.api.ratelimit.CachingEmbargoRuleCollectorImpl;
import com.oracle.pic.casper.webserver.api.ratelimit.Embargo;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoRuleCollector;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Impl;
import com.oracle.pic.casper.webserver.api.ratelimit.MdsEmbargoRuleCollectorImpl;
import com.oracle.pic.casper.webserver.api.sg.ServiceGateway;
import com.oracle.pic.casper.webserver.limit.ResourceLimiter;
import com.oracle.pic.casper.webserver.util.TestTenants;
import com.oracle.pic.casper.webserverv2.client.FakeServicePrincipalClientFactory;
import com.oracle.pic.casper.webserverv2.client.ServicePrincipalClientFactoryImpl;
import com.oracle.pic.commons.http.HttpRetryPolicy;
import com.oracle.pic.commons.util.AvailabilityDomain;
import com.oracle.pic.commons.util.Region;
import com.oracle.pic.identity.authentication.AuthenticatorClient;
import com.oracle.pic.identity.authentication.ServiceAuthenticationClient;
import com.oracle.pic.identity.authorization.sdk.AuthorizationClient;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.measure.unit.NonSI;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Authentication and authorization clients used by the web server.
 * <p>
 * See the comments on the skipAuth method for details on when authentication and authorization are skipped.
 */
public final class WebServerAuths {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebServerAuths.class);

    private static final String TEST_AUTH_KEY = "testAuth";
    private static final String SKIP_AUTH_KEY = "skipAuth";

    public static final String OLD_AUTHORIZATION_SERVICE_NAME = "Casper";

    private final AuthenticatorClient authnClient;
    //CASPER-5633 transitioning to new service name; this is the old client with "Casper" service name
    private final AuthorizationClient oldAuthzClient;
    //this is the new client with new service name
    private final AuthorizationClient newAuthzClient;

    private final Embargo embargo;
    private final EmbargoV3 embargoV3;

    private final Authenticator authenticator;
    private final AsyncAuthenticator asyncAuthenticator;
    private final S3Authenticator s3Authenticator;
    private final S3AsyncAuthenticator s3AsyncAuthenticator;
    private final Authorizer authorizer;

    private final AuthorizerPassThroughImpl authorizerPassThrough;

    private final ResourceControlledServicePrincipalClient resourceControlledServicePrincipalClient;

    private final AuthDataPlaneClient authDataPlaneClient;
    private final ResourceControlledMetadataClient metadataClient;
    private final Limits limits;

    private final String serviceName;

    /**
     * We use pass-through implementations for authentication and authorization if any of these conditions are true:
     * - The web server flavor is INTEGRATION_TESTS and -DtestAuth was not passed.
     * - The -DskipAuth argument was passed.
     * - The region is LOCAL and -DtestAuth was not passed.
     */
    public static boolean skipAuth(WebServerFlavor flavor, ConfigRegion region) {
        return (flavor == WebServerFlavor.INTEGRATION_TESTS && !Boolean.getBoolean(TEST_AUTH_KEY)) ||
                Boolean.getBoolean(SKIP_AUTH_KEY) ||
                (region == ConfigRegion.LOCAL && !Boolean.getBoolean(TEST_AUTH_KEY));
    }

    public WebServerAuths(
            WebServerFlavor flavor,
            CasperConfig config,
            WebServerClients clients,
            MdsClients mdsClients,
            ObjectMapper mapper,
            CertificateStore certStore,
            DecidingKeyManagementService kms,
            ResourceLimiter resourceLimiter) {
        final EmbargoRuleCollector embargoRuleCollector = (flavor == WebServerFlavor.INTEGRATION_TESTS) ?
            new MdsEmbargoRuleCollectorImpl(
                mdsClients.getOperatorMdsExecutor(),
                config.getMdsClientConfiguration().getOperatorMdsRequestDeadline()) :
            new CachingEmbargoRuleCollectorImpl(
                config.getWebServerConfigurations().getEmbargoConfiguration(),
                mdsClients.getOperatorMdsExecutor());
        embargo = new Embargo(embargoRuleCollector);
        embargoV3 = new EmbargoV3Impl(embargoRuleCollector);

        final long maxCopyRequests = config.getWorkRequestServiceConfigurations().getMaxRequestsPerTenant();
        final long maxBulkRestoreRequests = config.getWorkRequestServiceConfigurations().getMaxRequestsPerTenant();
        final long maxCopyBytes = config.getWorkRequestServiceConfigurations().getCopyConfiguration()
                .getMaxBytesPerTenant().longValue(NonSI.BYTE);

        serviceName = config.getCommonConfigurations().getServicePrincipalConfiguration().getCertCommonName();

        final CacheConfiguration authzCacheConfig = config.getAuthorizationClientConfiguration()
                .getCacheConfiguration();
        final CacheConfiguration v2AuthNCacheConfig = config.getV2AuthenticationClientConfiguration()
                .getCacheConfiguration();

        if (skipAuth(flavor, config.getRegion())) {
            authnClient = null;
            authenticator = new AuthenticatorPassThroughImpl(resourceLimiter);
            asyncAuthenticator = new AsyncAuthenticatorImpl(authenticator);
            s3Authenticator = new S3AuthenticatorPassThroughImpl(resourceLimiter);
            s3AsyncAuthenticator = new S3AsyncAuthenticator(s3Authenticator);
            oldAuthzClient = null;
            newAuthzClient = null;
            authorizer = new AuthorizerPassThroughImpl(embargo, resourceLimiter);
            resourceControlledServicePrincipalClient = new ResourceControlledServicePrincipalClient(
                    new FakeServicePrincipalClientFactory(), resourceLimiter);
            authDataPlaneClient = null;
            metadataClient = new ResourceControlledMetadataClient(
                    MetadataClient.fromInMemory(
                            AuthTestConstants.TENANTS_AND_COMPARTMENTS, config.getMetadataClientConfiguration()),
                    resourceLimiter);
            final InMemoryLimitsClient inMemoryLimitsClient = new InMemoryLimitsClient();
            inMemoryLimitsClient.setMaxCopyRequests(maxCopyRequests);
            inMemoryLimitsClient.setMaxBulkRestoreRequests(maxBulkRestoreRequests);
            inMemoryLimitsClient.setMaxCopyBytes(maxCopyBytes);
            inMemoryLimitsClient.setEventWhitelistedTenancies(new HashSet<>(
                    Arrays.asList(AuthTestConstants.FAKE_TENANT_ID)));
            limits = new Limits(config.getRegion(), inMemoryLimitsClient, maxCopyRequests, maxBulkRestoreRequests,
                    maxCopyBytes, Integer.MAX_VALUE, config.getLimitsClientConfiguration());
        } else {
            final CommonConfigurations commonConfigs = config.getCommonConfigurations();
            final IdentityEndpointConfiguration identityEndpointConfig = commonConfigs
                    .getServiceEndpointConfiguration().getIdentityEndpointConfiguration();
            String authDataPlaneUrl =
                    identityEndpointConfig.getDataEndpoint().getUrl();
            URI authDataPlaneUri =
                    identityEndpointConfig.getDataEndpoint().getUri();
            HttpHost authDataPlaneHttpHost = HttpHost.create(authDataPlaneUrl);

            final ServiceAuthenticationClient serviceAuthClient =
                    ServiceAuthenticationClientFactory.getServiceAuthenticationClient(
                            identityEndpointConfig.getStsEndpoint().getUrl(),
                            commonConfigs.getServicePrincipalConfiguration().getTenantId(),
                            commonConfigs.getServicePrincipalConfiguration().getCertDir(),
                            certStore);
            final ServiceAuthenticator serviceAuthenticator = new ServiceAuthenticatorImpl(
                    serviceAuthClient, config.getAuthDataPlaneClientConfiguration());
            final LimitsClient limitsClient;
            final SwiftAuthenticator swiftAuthenticator;
            final SwiftCredentialsClient swiftCredentialsClient;
            final SwiftLegacyAuthenticator swiftLegacyAuthenticator;

            //If we have testAuth enabled and we are LOCAL or LGL
            //we are not in the service enclave -- so use the public endpoint
            //for Identity but we will have to authenticate
            //with our service principal in order to do authenticate requests.
            //https://confluence.oci.oraclecorp.com/display/PLAT/How+To+Integrate+with+S2S
            if (config.getRegion() == ConfigRegion.LOCAL || config.getRegion() == ConfigRegion.LGL) {
                oldAuthzClient = AuthorizationClient.newThinClientBuilder()
                        .region(Region.SEA.getName())
                        .physicalAD(AvailabilityDomain.SEA_AD_1.getName())
                        .serviceName(OLD_AUTHORIZATION_SERVICE_NAME)
                        .decisionCacheRefresh(authzCacheConfig.getRefreshAfterWrite())
                        .decisionCacheExpire(authzCacheConfig.getExpireAfterWrite())
                        .decisionCacheMaximumSize(authzCacheConfig.getMaximumSize())
                        //@hbhuiyan -- 2018-06-21
                        //If AUTHORIZATION_SERVICE_NAME == Casper, then you cannot set serviceTeantnId,
                        //at least not until we whitelist that tenant id for Casper
                        //newer SDKs in fact ignore this field
                        //.serviceTenantId(commonConfigs.getServicePrincipalConfiguration().getTenantId())
                        .authorizationEndpoint(authDataPlaneUrl)
                        .serviceAuthenticationClient(serviceAuthClient)
                        .build();
                newAuthzClient = AuthorizationClient.newThinClientBuilder()
                        .region(Region.SEA.getName())
                        .physicalAD(AvailabilityDomain.SEA_AD_1.getName())
                        .serviceName(serviceName)
                        .decisionCacheRefresh(authzCacheConfig.getRefreshAfterWrite())
                        .decisionCacheExpire(authzCacheConfig.getExpireAfterWrite())
                        .decisionCacheMaximumSize(authzCacheConfig.getMaximumSize())
                        .authorizationEndpoint(authDataPlaneUrl)
                        .serviceAuthenticationClient(serviceAuthClient)
                        .build();

                // The AuthenticatorClient and AuthDataPlaneClient are for the same service, so they share the same
                // retry/backoff behavior, see the documentation in AuthDataPlaneClientApacheImpl for details.
                authnClient = new AuthenticatorClient.Builder()
                        .userKeyCacheMaximumSize(v2AuthNCacheConfig.getMaximumSize())
                        .userKeyCacheRefresh(v2AuthNCacheConfig.getRefreshAfterWrite())
                        .userKeyCacheExpire(v2AuthNCacheConfig.getExpireAfterWrite())
                        .authServiceKeyCacheMaximumSize(v2AuthNCacheConfig.getMaximumSize())
                        .authServiceKeyCacheRefresh(v2AuthNCacheConfig.getRefreshAfterWrite())
                        .authServiceKeyCacheExpire(v2AuthNCacheConfig.getExpireAfterWrite())
                        .keyRetrievalMaxDuration(Duration.ofSeconds(10))
                        .keyServiceUrl(authDataPlaneUri)
                        .serviceAuthenticationClient(serviceAuthClient)
                        .keyRetrievalMaxElapsedTime(AuthDataPlaneClientApacheImpl.MAX_ELAPSED_TIME)
                        .keyRetrievalMaxDuration(AuthDataPlaneClientApacheImpl.SOCKET_TIMEOUT)
                        .keyRetrievalInitialBackOff(AuthDataPlaneClientApacheImpl.INITIAL_BACKOFF)
                        .multiplier(AuthDataPlaneClientApacheImpl.BACKOFF_MULT)
                        .build();
                limitsClient = new InMemoryLimitsClient();

                authDataPlaneClient = new AuthDataPlaneClientApacheImpl(
                        authDataPlaneHttpHost, mapper, serviceAuthenticator);
                swiftCredentialsClient = new SwiftCredentialsClientImpl(
                        authDataPlaneClient,
                        config.getAuthDataPlaneClientConfiguration(), config.getSwiftCredentialsClientConfiguration());
                swiftLegacyAuthenticator = new SwiftLegacyAuthenticatorImpl(
                        authDataPlaneClient, config.getAuthDataPlaneClientConfiguration());
                swiftAuthenticator = new SwiftAuthenticatorImpl(config, swiftCredentialsClient,
                        swiftLegacyAuthenticator);

                metadataClient = new ResourceControlledMetadataClient(
                        MetadataClient.fromIdentityClient(authDataPlaneUrl, HttpRetryPolicy.DEFAULT_RETRY_POLICY,
                                serviceAuthClient, config.getMetadataClientConfiguration()),
                        resourceLimiter);
            } else {
                authnClient = new AuthenticatorClient.Builder()
                        .userKeyCacheMaximumSize(v2AuthNCacheConfig.getMaximumSize())
                        .userKeyCacheRefresh(v2AuthNCacheConfig.getRefreshAfterWrite())
                        .userKeyCacheExpire(v2AuthNCacheConfig.getExpireAfterWrite())
                        .authServiceKeyCacheMaximumSize(v2AuthNCacheConfig.getMaximumSize())
                        .authServiceKeyCacheRefresh(v2AuthNCacheConfig.getRefreshAfterWrite())
                        .authServiceKeyCacheExpire(v2AuthNCacheConfig.getExpireAfterWrite())
                        .keyRetrievalMaxDuration(Duration.ofSeconds(10))
                        .keyServiceUrl(authDataPlaneUri)
                        .build();
                Region region = config.getRegion().toRegion();
                AvailabilityDomain ad = AvailabilityDomains.is(region, config.getAvailabilityDomain());
                oldAuthzClient = AuthorizationClient.newThinClientBuilder()
                        .region(region.getName())
                        .physicalAD(ad.getName())
                        .serviceName(OLD_AUTHORIZATION_SERVICE_NAME)
                        .authorizationEndpoint(authDataPlaneUrl)
                        .decisionCacheRefresh(authzCacheConfig.getRefreshAfterWrite())
                        .decisionCacheExpire(authzCacheConfig.getExpireAfterWrite())
                        .decisionCacheMaximumSize(authzCacheConfig.getMaximumSize())
                        .build();
                newAuthzClient = AuthorizationClient.newThinClientBuilder()
                        .region(region.getName())
                        .physicalAD(ad.getName())
                        .serviceName(serviceName)
                        .authorizationEndpoint(authDataPlaneUrl)
                        .decisionCacheRefresh(authzCacheConfig.getRefreshAfterWrite())
                        .decisionCacheExpire(authzCacheConfig.getExpireAfterWrite())
                        .decisionCacheMaximumSize(authzCacheConfig.getMaximumSize())
                        .build();
                limitsClient = new IdentityLimitsClient(null,
                        identityEndpointConfig.getLimitsEndpoint().getUrl(), HttpRetryPolicy.DEFAULT_RETRY_POLICY);
                authDataPlaneClient = new AuthDataPlaneClientApacheImpl(authDataPlaneHttpHost, mapper, null);
                swiftCredentialsClient = new SwiftCredentialsClientImpl(
                        authDataPlaneClient, config.getAuthDataPlaneClientConfiguration(),
                        config.getSwiftCredentialsClientConfiguration());
                swiftLegacyAuthenticator = new SwiftLegacyAuthenticatorImpl(
                        authDataPlaneClient, config.getAuthDataPlaneClientConfiguration());
                swiftAuthenticator = new SwiftAuthenticatorImpl(config, swiftCredentialsClient,
                        swiftLegacyAuthenticator);
                metadataClient = new ResourceControlledMetadataClient(
                        MetadataClient.fromIdentityClient(
                                authDataPlaneUrl, HttpRetryPolicy.DEFAULT_RETRY_POLICY, null,
                                config.getMetadataClientConfiguration()),
                        resourceLimiter);
            }

            JWTAuthenticator jwtAuthenticator = JWTAuthenticator.preloadFromResourceFile();
            authenticator = new AuthenticatorImpl(
                    authnClient, metadataClient, config.getAuthDataPlaneClientConfiguration(), swiftAuthenticator,
                    jwtAuthenticator, resourceLimiter);

            asyncAuthenticator = new AsyncAuthenticatorImpl(authenticator);
            final S3SigningKeyClient s3SigningKeyClient = new S3SigningKeyClientImpl(
                    authDataPlaneClient, config.getAuthDataPlaneClientConfiguration(),
                    config.getS3SigningKeyClientConfiguration());
            s3Authenticator = new S3AuthenticatorImpl(s3SigningKeyClient, metadataClient, resourceLimiter);
            s3AsyncAuthenticator = new S3AsyncAuthenticator(s3Authenticator);


            final ServiceGatewayConfiguration sgwConfig =
                    config.getWebServerConfigurations().getServiceGatewayConfiguration();
            final ServiceGateway serviceGateway = new ServiceGateway(sgwConfig, clients.getVcnIdProvider());
            limits = new Limits(config.getRegion(), limitsClient, maxCopyRequests, maxBulkRestoreRequests, maxCopyBytes,
                    Integer.MAX_VALUE, config.getLimitsClientConfiguration());
            final TestTenants testTenants = new TestTenants(config.getWebServerConfigurations().getTestTenancies());
            authorizer = new AuthorizerImpl(oldAuthzClient, newAuthzClient, metadataClient, kms, limits, embargo,
                    serviceGateway, testTenants, serviceName,
                    new AuthDataPlaneFailSafeConfig(
                            config.getAuthDataPlaneClientConfiguration().getFailSafeConfiguration()),
                    resourceLimiter);

            final HttpClientConfiguration httpClientConfig =
                    config.getCommonConfigurations().getHttpClientConfiguration();
            resourceControlledServicePrincipalClient = new ResourceControlledServicePrincipalClient(
                    ServicePrincipalClientFactoryImpl.createFactory(
                            httpClientConfig.getConnectionTimeout(),
                            httpClientConfig.getIdleTimeout(),
                            serviceAuthenticator,
                            config.getCommonConfigurations().getServicePrincipalConfiguration().getCertCommonName()),
                    resourceLimiter);
        }
        authorizerPassThrough = new AuthorizerPassThroughImpl(embargo, resourceLimiter);
    }

    public Embargo getEmbargo() {
        return embargo;
    }

    public EmbargoV3 getEmbargoV3() {
        return embargoV3;
    }

    public Authenticator getAuthenticator() {
        return authenticator;
    }

    public AsyncAuthenticator getAsyncAuthenticator() {
        return asyncAuthenticator;
    }

    public S3Authenticator getS3Authenticator() {
        return s3Authenticator;
    }

    public S3AsyncAuthenticator getS3AsyncAuthenticator() {
        return s3AsyncAuthenticator;
    }

    public Authorizer getAuthorizer() {
        return authorizer;
    }

    public Limits getLimits() {
        return limits;
    }

    public AuthorizerPassThroughImpl getAuthorizerPassThrough() {
        return authorizerPassThrough;
    }

    public ResourceControlledServicePrincipalClient getResourceControlledServicePrincipalClient() {
        return resourceControlledServicePrincipalClient;
    }

    public void close() {
        if (authnClient != null) {
            try {
                authnClient.close();
            } catch (Exception ex) {
                LOGGER.warn("Failed to close the authentication client", ex);
            }
        }

        if (oldAuthzClient != null) {
            try {
                oldAuthzClient.close();
            } catch (Exception ex) {
                LOGGER.warn("Failed to close the old authorization client", ex);
            }
        }

        if (newAuthzClient != null) {
            try {
                newAuthzClient.close();
            } catch (Exception ex) {
                LOGGER.warn("Failed to close the new authorization client", ex);
            }
        }

        if (authDataPlaneClient != null) {
            try {
                authDataPlaneClient.close();
            } catch (Exception ex) {
                LOGGER.warn("Failed to close the dataplane client", ex);
            }
        }

        if (embargo != null) {
            embargo.getRuleCollector().shutdown();
        }
    }

    public ResourceControlledMetadataClient getMetadataClient() {
        return metadataClient;
    }

    private EmbargoRuleCollector getEmbargoRuleCollector(
            WebServerFlavor flavor,
            CasperConfig config,
            MdsClients mdsClients) {
        final EmbargoRuleCollector embargoRuleCollector;
        if (flavor == WebServerFlavor.INTEGRATION_TESTS) {
            embargoRuleCollector = new MdsEmbargoRuleCollectorImpl(
                mdsClients.getOperatorMdsExecutor(),
                config.getMdsClientConfiguration().getOperatorMdsRequestDeadline());
        } else {
            embargoRuleCollector = new CachingEmbargoRuleCollectorImpl(
                config.getWebServerConfigurations().getEmbargoConfiguration(),
                mdsClients.getOperatorMdsExecutor());
        }
        return embargoRuleCollector;
    }
}
