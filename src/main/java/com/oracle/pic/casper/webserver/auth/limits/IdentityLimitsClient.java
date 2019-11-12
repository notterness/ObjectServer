package com.oracle.pic.casper.webserver.auth.limits;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.util.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.oracle.pic.accounts.model.CompartmentQuotasAndServiceLimits;
import com.oracle.pic.accounts.model.ListCompartmentsQuotasRequest;
import com.oracle.pic.casper.common.auth.dataplane.ServiceAuthenticator;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.exceptions.LimitsServerException;
import com.oracle.pic.casper.webserver.util.ObjectMappers;
import com.oracle.pic.commons.http.HttpClient;
import com.oracle.pic.commons.http.HttpRetryPolicy;
import com.oracle.pic.commons.http.RetryableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * See https://bitbucket.oci.oraclecorp.com/projects/LIM/repos/properties-runtime-client/browse/properties-runtime-api-spec/src/main/resources/properties-runtime-api-spec.yaml
 * for API spec.
 */
public class IdentityLimitsClient implements LimitsClient {

    private static final Logger LOG = LoggerFactory.getLogger(IdentityLimitsClient.class);

    private static final String LIMITS_OPC_SERVICE = "casper";
    private static final String PUBLIC_BUCKET_ENABLED_PROPERTY_NAME = "public-bucket-enabled";
    private static final String PUBLIC_BUCKET_ENABLED_PROPERTY_VALUE = "enabled";
    private static final String MAX_COPY_REQUEST_PROPERTY_NAME = "max-copy-request";
    private static final String MAX_COPY_BYTES_PROPERTY_NAME = "max-copy-bytes";
    private static final String MAX_BULK_RESTORE_REQUEST_PROPERTY_NAME = "max-bulk-restore-request";
    private static final String MAX_BUCKETS_REQUEST_LIMIT_NAME = "max-buckets";
    private static final String MAX_BUCKET_SHARD_REQUEST_LIMIT_NAME = "bucket-shards";
    private static final String MAX_STORAGE_BYTES_REQUEST_LIMIT_NAME = "storage-limit-bytes";

    //this client is used to test whether a tenant is allowed to do something - limits service returns a boolean
    private final RetryableHttpClient<Void, Boolean> evaluateClient;
    //this client is used to fetch property values from limits service such as the maximum number of copies allowed
    private final RetryableHttpClient<Void, Property> propertyClient;
    //this client is used to fetch service limit and compartment quotas from limits service
    private final RetryableHttpClient<ListCompartmentsQuotasRequest, CompartmentQuotasAndServiceLimits> quotaClient;

    private ServiceAuthenticator serviceAuthenticator;
    private final String limitsEndpoint;

    /**
     * Shortname of the region (e.g. "PHX", "SEA", "IAD").
     */
    private final String regionAirportCode;

    public IdentityLimitsClient(@Nullable ServiceAuthenticator serviceAuthenticator,
                                String limitsEndpoint,
                                HttpRetryPolicy retryPolicy) {
        this(serviceAuthenticator, limitsEndpoint, ConfigRegion.fromSystemProperty().getAirportCode(), retryPolicy);
    }

    IdentityLimitsClient(@Nullable ServiceAuthenticator serviceAuthenticator,
                                String limitsEndpoint,
                                String regionAirportCode,
                                HttpRetryPolicy retryPolicy) {
        this.serviceAuthenticator = serviceAuthenticator;
        this.limitsEndpoint = limitsEndpoint;
        this.regionAirportCode = regionAirportCode;
        this.evaluateClient = new RetryableHttpClient<>(Boolean.class, retryPolicy);
        this.propertyClient = new RetryableHttpClient<>(Property.class, retryPolicy);
        this.quotaClient = new RetryableHttpClient<>(CompartmentQuotasAndServiceLimits.class, retryPolicy);
    }

    @Override
    public boolean isSuspended(String tenantId) {
        final URI requestUri = UriBuilder.fromUri(limitsEndpoint)
                .path("limits")
                .path("suspended")
                .path("tag")
                .path(tenantId)
                .build();

        Map<String, String> headers = Maps.newHashMap();
        headers.put("opc-guid", "");
        headers.put("opc-service", LIMITS_OPC_SERVICE);
        signHeadersForServiceAuth("GET", requestUri, headers, null);

        try {
            return evaluateClient.getResponse(
                    requestUri,
                    HttpClient.RequestType.GET,
                    Optional.empty(),
                    Optional.of(headers));
        } catch (HttpResponseException e) {
            throw new LimitsServerException(e.getStatusCode(), e.getStatusMessage(), e);
        } catch (IOException e) {
            throw new LimitsServerException(500, "UnknownFailure", e);
        }
    }

    @Override
    public boolean isPublicBucketEnabled(String tenantId) {
        return evaluateProperty(tenantId, PUBLIC_BUCKET_ENABLED_PROPERTY_NAME, PUBLIC_BUCKET_ENABLED_PROPERTY_VALUE);
    }

    @Override
    public long getMaxCopyRequests(String tenantId) {
        return getProperty(tenantId, MAX_COPY_REQUEST_PROPERTY_NAME).getMax();
    }

    @Override
    public long getMaxCopyBytes(String tenantId) {
        return getProperty(tenantId, MAX_COPY_BYTES_PROPERTY_NAME).getMax();
    }

    @Override
    public Set<String> getEventWhitelistedTenancies(String whitelistedService) {
        // path:   /properties/value/group/cloudevents-stage/property/tenantWhitelist :
        // this will return tenant property for realm (global). If we ever want to change this
        // per region property - depending upon customer requirement - we will need to change path.
        // per region api is available
        String cloudEventsPath;
        ConfigRegion region = ConfigRegion.tryFromSystemProperty().orElse(ConfigRegion.LOCAL);
        if (region == ConfigRegion.LOCAL || region == ConfigRegion.LGL ||
                region == ConfigRegion.R1_STABLE || region == ConfigRegion.R1_UNSTABLE) {
            cloudEventsPath = "cloudevents";
        } else {
            cloudEventsPath = "cloudevents-prod";
        }
        final URI requestUri = UriBuilder.fromUri(limitsEndpoint)
                .path("properties")
                .path("value")
                .path("group")
                .path(cloudEventsPath)
                .path("property")
                .path("tenantWhitelist")
                .path("tag")
                .path("default")
                .build();

        Map<String, String> headers = Maps.newHashMap();
        headers.put("opc-guid", "");
        headers.put("opc-service", LIMITS_OPC_SERVICE);
        signHeadersForServiceAuth("GET", requestUri, headers, null);

        try {
            Property prop =  propertyClient.getResponse(
                    requestUri,
                    HttpClient.RequestType.GET,
                    Optional.empty(),
                    Optional.of(headers));
            String whitelist = prop.getValue();
            // convert the list of string of ocid into set of string
            if (whitelist.charAt(0) == '[' && whitelist.charAt(whitelist.length() - 1) == ']') {
                whitelist = whitelist.substring(1, whitelist.length() - 1);
            }
            String trimmedWhitelist = whitelist.replace("\"", "").replace(" ", "");
            return new HashSet<String>(Arrays.asList(trimmedWhitelist.split(",")));
        } catch (HttpResponseException e) {
            throw new LimitsServerException(e.getStatusCode(), e.getStatusMessage(), e);
        } catch (IOException e) {
            throw new LimitsServerException(500, "UnknownFailure", e);
        }
    }

    @Override
    public long getMaxBulkRestoreRequests(String tenantId) {
        return getProperty(tenantId, MAX_BULK_RESTORE_REQUEST_PROPERTY_NAME).getMax();
    }

    @Override
    public long getMaxBuckets(String tenantId) {
        return getLimit(tenantId, MAX_BUCKETS_REQUEST_LIMIT_NAME).getMax();
    }

    @Override
    public long getBucketShards(String tenantId) {
        return getLimit(tenantId, MAX_BUCKET_SHARD_REQUEST_LIMIT_NAME).getMax();
    }

    @Override
    public long getStorageLimitBytes(String tenantId) {
        return getLimit(tenantId, MAX_STORAGE_BYTES_REQUEST_LIMIT_NAME).getMax();
    }

    @Override
    public CompartmentQuotasAndServiceLimits getQuotasAndServiceLimits(ListCompartmentsQuotasRequest request) {
        return getQuota(request);
    }

    /**
     * How do we look up the limit for a tenancy? We want to support per-region
     * overrides so we use this path:
     *  /limits/value/group/{group}/limit/{limit}/tag/{tag}/region/{region}
     * Where:
     *  group: is always "casper"
     *  limit: the name of the limit (e.g. "max-buckets").
     *  tag: tenant OCID
     *  region: shortname of the region (e.g. SEA, or PHX). This does
     *          not seem to be case-sensitive.
     *
     * The limits can also be looked up using curl. For example in R1:
     *   http://limits.svc.ad2.r1:80/20180322/limits/value/group/casper/limit/max-buckets/tag/ocid1.tenancy.region1..aaaaaaaak76j3v6mudblv7szlimvbcs7pqp6y4azrep3exhqi4qvsquyzpqa/region/sea
     * And in us-phoenix-1:
     *   http://limits.svc.ad1.r2:80/20180322/limits/value/group/casper/limit/max-buckets/tag/ocid1.tenancy.oc1..aaaaaaaab42wwm5vbhjgzie4vkw6rtdwzzndlwcpog4fewoe3yzgvcf6dvza/region/phx
     */
    private Property getLimit(String tenantId, String limitName) {
        final URI requestUri = UriBuilder.fromUri(limitsEndpoint)
            .path("limits")
            .path("value")
            .path("group")
            .path("casper")
            .path("limit")
            .path(limitName)
            .path("tag")
            .path(tenantId)
            .path("region")
            .path(regionAirportCode)
            .build();

        final Map<String, String> headers = Maps.newHashMap();
        headers.put("opc-guid", "");
        headers.put("opc-service", LIMITS_OPC_SERVICE);
        signHeadersForServiceAuth("GET", requestUri, headers, null);

        try {
            return propertyClient.getResponse(
                requestUri,
                HttpClient.RequestType.GET,
                Optional.empty(),
                Optional.of(headers));
        } catch (HttpResponseException e) {
            throw new LimitsServerException(e.getStatusCode(), e.getStatusMessage(), e);
        } catch (IOException e) {
            throw new LimitsServerException(500, "UnknownFailure", e);
        }
    }

    /**
     * To retrieve service limits and quotas we make a POST api call this path:
     *  /limits/compartments/list
     * The request includes the tenantId, compartmentIds, quota names and region name in the context
     *
     * The limits can also be looked up using curl. For example in R1:
     *   curl --header "Content-Type: application/json" --data "@quotaRequest"
     *   http://limits.svc.ad2.r1:80/20180322/limits/compartments/list
     *
     * quotaRequest:
     * {
     *   "tenantId":"ocid1.tenancy.region1..aaaaaaaal5xu7tathwh5opyxouzt24tgl7qff7td4i6zb4sbqzoe4iusbyja",
     *   "family":"object-storage",
     *   "quotaNames": ["storage-bytes"],
     *   "compartmentIds":["ocid1.tenancy.region1..aaaaaaaal5xu7tathwh5opyxouzt24tgl7qff7td4i6zb4sbqzoe4iusbyja"],
     *   "context":[{"name":"request.region", "value":"us-seattle-1", "type":"STRING"}]
     * }
     */
    private CompartmentQuotasAndServiceLimits getQuota(ListCompartmentsQuotasRequest request) {
        final URI requestUri = UriBuilder.fromUri(limitsEndpoint)
                .path("limits")
                .path("compartments")
                .path("list")
                .build();

        final Map<String, String> headers = Maps.newHashMap();

        headers.put("opc-guid", "");
        headers.put("opc-service", LIMITS_OPC_SERVICE);
        ObjectMappers.createCasperObjectMapper();


        try {
            String s = new ObjectMapper().writeValueAsString(request);
            signHeadersForServiceAuth("POST", requestUri, headers, s);

            return quotaClient.getResponse(
                    requestUri,
                    HttpClient.RequestType.POST,
                    Optional.of(request),
                    Optional.of(headers));
        } catch (HttpResponseException e) {
            throw new LimitsServerException(e.getStatusCode(), e.getStatusMessage(), e);
        } catch (IOException e) {
            throw new LimitsServerException(500, "UnknownFailure", e);
        }
    }

    /**
     * Evaluate checks if a given value conforms to the property (i.e.
     * for numeric limits it checks that it is between min and max, and for
     * string limits it checks if it is the exact string). The result is true
     * or false depending if they match or not the condition.
     *
     * This is used for the public-bucket property where we want to know
     * if the string is exactly "enabled".
     *
     * The path for this is:
     *   /properties/evaluate/group/{group}/property/{property}/tag/{tag}/value/{value}/region/{region}
     */
    private boolean evaluateProperty(String tenantId, String propertyName, String propertyValue) {
        final URI requestUri = UriBuilder.fromUri(limitsEndpoint)
            .path("properties")
            .path("evaluate")
            .path("group")
            .path("casper")
            .path("property")
            .path(propertyName)
            .path("tag")
            .path(tenantId)
            .path("value")
            .path(propertyValue)
            .path("region")
            .path(regionAirportCode)
            .build();

        final Map<String, String> headers = Maps.newHashMap();
        headers.put("opc-guid", "");
        headers.put("opc-service", LIMITS_OPC_SERVICE);
        signHeadersForServiceAuth("GET", requestUri, headers, null);

        try {
            return evaluateClient.getResponse(
                requestUri,
                HttpClient.RequestType.GET,
                Optional.empty(),
                Optional.of(headers));
        } catch (HttpResponseException e) {
            throw new LimitsServerException(e.getStatusCode(), e.getStatusMessage(), e);
        } catch (IOException e) {
            throw new LimitsServerException(500, "UnknownFailure", e);
        }
    }

    /**
     * Retrieve the value of a property. The path is:
     *   /properties/value/group/{group}/property/{property}/tag/{tag}/region/{region}
     */
    private Property getProperty(String tenantId, String propertyName) {
        final URI requestUri = UriBuilder.fromUri(limitsEndpoint)
            .path("properties")
            .path("value")
            .path("group")
            .path("casper")
            .path("property")
            .path(propertyName)
            .path("tag")
            .path(tenantId)
            .path("region")
            .path(regionAirportCode)
            .build();

        final Map<String, String> headers = Maps.newHashMap();
        headers.put("opc-guid", "");
        headers.put("opc-service", LIMITS_OPC_SERVICE);
        signHeadersForServiceAuth("GET", requestUri, headers, null);

        //Limits service throws a 404 if property is not found, and return the default value if tenant is unknown
        try {
            return propertyClient.getResponse(
                    requestUri,
                    HttpClient.RequestType.GET,
                    Optional.empty(),
                    Optional.of(headers));
        } catch (HttpResponseException e) {
            throw new LimitsServerException(e.getStatusCode(), e.getStatusMessage(), e);
        } catch (IOException e) {
            throw new LimitsServerException(500, "UnknownFailure", e);
        }
    }

    private void signHeadersForServiceAuth(String method, URI requestUri, Map<String, String> headers, String body) {
        if (serviceAuthenticator != null) {
            // If you are a service running outside service enclave then you need s2s auth
            LOG.info("Getting signed headers to make s2s auth-service call");
            final byte[] bytes = body == null ? null : body.getBytes(Charsets.UTF_8);
            Map<String, String> serviceAuthenticationHeaders =
                    serviceAuthenticator.getSignedRequestHeaders(
                            method,
                            requestUri,
                            ImmutableMap.of(),
                            Optional.ofNullable(bytes));
            headers.putAll(serviceAuthenticationHeaders);
        }
    }

    @Override
    public void close() throws IOException {
    }

}
