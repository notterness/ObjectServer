package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.model.ObjectSummaryCollection;
import com.oracle.pic.casper.webserver.api.model.ObjectProperties;
import com.oracle.pic.casper.webserver.api.model.serde.ObjectSummary;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.s3.model.ListVersionsResult;
import com.oracle.pic.casper.webserver.api.s3.model.Owner;
import com.oracle.pic.casper.webserver.api.s3.util.S3XmlResponseUtil;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.casper.webserver.util.WebServerMetrics;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang.time.DateUtils;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;


/**
 * A GET versions request expects back metadata of all the versions of objects in the bucket; containing keys and their
 * "VersionId"s (among other parameters). Casper currently doesn't support versioning, so we 'fake' the version-ids
 * here in the handler, considering all objects to have only a single version, and generating a version-id for each key.
 *
 * Version-ids are used both in the request and response. In the request a 'version-id-marker' parameter specifies the
 * object version to begin listing from. In the response version-ids are returned for each object, as well as used in
 * the NextVersionIdMarker and VersionIdMarker parameters.
 *
 * Faking VersionIds: The version-id <-> key mapping needs to be two way, as in each should be able to be generated
 * from the other. This is because the Casper backend has no concept of versions (yet), and the handler needs to be
 * able to generate version-ids from keys, and vice versa. For eg, given a request of versions, the handler will
 * retrieve the key listing from the Backend and generate version-ids for the response. And similarly, given
 * a request containing a 'version-id-marker' the handler has to generate the corresponding key to be able
 * to query the back end.
 *
 * The ObjectKeyToVersionIdBiMapper handles the conversion to of a key to a version-id, and vice-versa.
 *
 */

public class GetBucketObjectVersionsHandler extends SyncHandler {

    private final S3Authenticator authenticator;
    private final Backend backend;
    private final XmlMapper mapper;
    private final EmbargoV3 embargoV3;
    private static final ImmutableList<ObjectProperties> OBJECT_PROPERTIES = ImmutableList.of(ObjectProperties.NAME,
            ObjectProperties.MD5, ObjectProperties.TIMEMODIFIED, ObjectProperties.SIZE,
            ObjectProperties.ARCHIVED);

    public static class ListBucketObjectVersionsRequest {

        private String versionIdMarker;
        private String keyMarker;
        private final String namespace;
        private final String bucketName;
        private final int backendLimit;
        private final String delimiter;
        private final String encodingType;
        private final String prefix;
        private final String contentSha256;

        static final int DEFAULT_LIMIT = 1000;

        public ListBucketObjectVersionsRequest(HttpServerRequest request, RoutingContext context) {
            final WSRequestContext wsRequestContext = WSRequestContext.get(context);

            namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
            bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);

            //we only support 1 character delimiter
            this.delimiter = S3HttpHelpers.getDelimiter(request);

            this.backendLimit = S3HttpHelpers.getMaxKeys(request).orElse(DEFAULT_LIMIT);
            this.encodingType = S3HttpHelpers.getEncoding(request).orElse(null);
            this.prefix = S3HttpHelpers.getPrefix(request);
            this.contentSha256 = S3HttpHelpers.getContentSha256(request);

            this.keyMarker = S3HttpHelpers.getKeyMarker(request);
            this.versionIdMarker = S3HttpHelpers.getVersionIdMarker(request);

            // if a keyMarker doesn't exist and a version-id-marker exists, throw up
            if (this.keyMarker == null && this.versionIdMarker != null) {
                throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT,
                        "A version-id marker cannot be specified without a key marker.", request.path());
            }
        }

        public String getContentSha256() {
            return contentSha256;
        }

        public String getPrefix() {
            return prefix;
        }

        public String getKeyMarker() {
            return keyMarker;
        }

        public String getEncodingType() {
            return encodingType;
        }

        public String getNamespace() {
            return namespace;
        }

        public String getBucketName() {
            return bucketName;
        }

        public int getBackendLimit() {
            return backendLimit;
        }

        public String getDelimiter() {
            return delimiter;
        }

        public String getVersionIdMarker() {
            return versionIdMarker;
        }
    }


    public GetBucketObjectVersionsHandler(
            S3Authenticator authenticator, Backend backend, XmlMapper mapper, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.backend = backend;
        this.mapper = mapper;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.GET_BUCKET);
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_GET_BUCKET_BUNDLE);

        final HttpServerRequest request = context.request();

        // Retrieve parameters from, and validate, request
        final ListBucketObjectVersionsRequest versionsRequest = new ListBucketObjectVersionsRequest(request, context);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.GET_BUCKET)
            .setNamespace(versionsRequest.getNamespace())
            .setBucket(versionsRequest.getBucketName())
            .build();
        embargoV3.enter(embargoOperation);

        S3HttpHelpers.failSigV2(request);
        S3HttpHelpers.validateEmptySha256(versionsRequest.getContentSha256(), request);

        try {
            final AuthenticationInfo authInfo = authenticator.authenticate(context, versionsRequest.getContentSha256());

            //we only support 1 character delimiter
            final String delimiter = versionsRequest.getDelimiter();
            final Character delimitChar = (delimiter != null && delimiter.length() == 1) ? delimiter.charAt(0) : null;

            // Number of objects we are requesting.
            final int limit = Math.min(
                ListBucketObjectVersionsRequest.DEFAULT_LIMIT,
                versionsRequest.getBackendLimit());

            final ObjectSummaryCollection summary;
            if (limit == 0) {
                summary = new ObjectSummaryCollection(ImmutableList.of(), versionsRequest.getBucketName(),
                        ImmutableList.of(), (ObjectSummary) null, null, null);
            } else {
                // The S3 list versions api supports a "start after" parameter, but listObjects takes a "start with".
                // We need to convert "start after" to "start with"
                final String startWith = KeyUtils.startWith(
                        versionsRequest.getKeyMarker(),
                        versionsRequest.getPrefix(),
                        delimitChar);
                summary = backend.listObjects(context,
                        authInfo,
                        OBJECT_PROPERTIES,
                        limit,
                        versionsRequest.getNamespace(),
                        versionsRequest.getBucketName(),
                        delimitChar,
                        versionsRequest.getPrefix(),
                        startWith,
                        null);
            }

            context.response().putHeader(S3Api.BUCKET_REGION, ConfigRegion.fromSystemProperty().getFullName());

            final S3XmlResult xmlResult = serializeBucketObjectVersions(versionsRequest, summary, authInfo);
            S3HttpHelpers.writeXmlResponse(context.response(), xmlResult, mapper);
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }

    }

    /**
     * Serialize the results from the Backend into XML for the response
     */
    static ListVersionsResult serializeBucketObjectVersions(
            ListBucketObjectVersionsRequest versionsRequest,
            ObjectSummaryCollection objectSummaryCollection,
            AuthenticationInfo authInfo) {

        // How do we want to encode strings
        final Function<String, String> encoder;
        if ("url".equalsIgnoreCase(versionsRequest.getEncodingType())) {
            encoder = s -> urlEncode(s);
        } else {
            encoder = s -> S3XmlResponseUtil.sanitizeForXml(s);
        }

        // Owner Info
        final Owner owner = new Owner(authInfo.getMainPrincipal());

        // Backend can return too many objects so we have to post-process here
        // Count the number of results we get.
        int results = 0;

        // Set to the name of the last object or prefix we used
        String nextKeyMarker = null;
        String nextVersionIdMarker = null;

        // Results we got back from Backend
        final Queue<ObjectSummary> objects = Queues.newArrayDeque(
            objectSummaryCollection.getObjects() != null ? objectSummaryCollection.getObjects() : ImmutableList.of());
        final Queue<String> prefixes = Queues.newArrayDeque(
            objectSummaryCollection.getPrefixes() != null ? objectSummaryCollection.getPrefixes() : ImmutableList.of());

        // What we will return to the caller
        final List<ListVersionsResult.Version> versions = new ArrayList<>();
        final List<ListVersionsResult.Prefix> commonPrefixes = new ArrayList<>();

        // Go through the returned results alphabetically until we run out of results or
        // hit our limit.
        while ((!objects.isEmpty() || !prefixes.isEmpty()) && results < versionsRequest.getBackendLimit()) {
            if (!objects.isEmpty() && (prefixes.isEmpty() || objects.peek().getName().compareTo(prefixes.peek()) < 0)) {
                // We have an object and either no prefixes or a greater prefix (we want the smaller one)
                final ObjectSummary objectSummary = objects.remove();
                final ListVersionsResult.Version version = new ListVersionsResult.Version(
                    encoder.apply(objectSummary.getName()),
                    DateUtils.truncate(objectSummary.getTimeModified(), Calendar.SECOND),
                    objectSummary.getChecksum().getQuotedHex(),
                    objectSummary.getSize(),
                    S3StorageClass.STANDARD,
                    owner,
                    "null"); // S3 uses "null" as the version ID for non-versioned objects
                versions.add(version);
                nextKeyMarker = version.getKey();
                nextVersionIdMarker = version.getVersionId();
            } else {
                // We must have a prefix we want to use
                final ListVersionsResult.Prefix prefix = new ListVersionsResult.Prefix(
                    encoder.apply(prefixes.remove()));
                commonPrefixes.add(prefix);
                nextKeyMarker = prefix.getPrefix();
                nextVersionIdMarker = null;
            }

            ++results;
            assert results > 0;
            assert nextKeyMarker != null;
        }

        // Set results
        ListVersionsResult.Builder builder = new ListVersionsResult.Builder()
            .name(objectSummaryCollection.getBucketName())
            .keyMarker(encoder.apply(versionsRequest.getKeyMarker()))
            .versionIdMarker(versionsRequest.getVersionIdMarker())
            .prefix(encoder.apply(versionsRequest.getPrefix()))
            .delimiter(encoder.apply(versionsRequest.getDelimiter()))
            .maxKeys(versionsRequest.getBackendLimit())
            .encodingType(versionsRequest.getEncodingType())
            .versions(versions)
            .commonPrefixes(commonPrefixes);

        // Are the results truncated? There are two ways the results could be truncated:
        //  1) The ObjectSummaryCollection tells us there are more results.
        //  2) We did not use all the results we were given (i.e. we hit the results limit).
        // We will always use the key of the last object/prefix we are returning as the next key marker.
        if (objectSummaryCollection.getNextStartWith() != null || !objects.isEmpty() || !prefixes.isEmpty()) {
            builder = builder.nextKeyMarker(nextKeyMarker) // already encoded
                .nextVersionIdMarker(nextVersionIdMarker)
                .isTruncated(true);
        } else {
            builder = builder.nextKeyMarker(null)
                .nextVersionIdMarker(null)
                .isTruncated(false);
        }

        return builder.build();
    }

    private static String urlEncode(@Nullable String unencoded) {
        try {
            return unencoded == null ? null : URLEncoder.encode(unencoded, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
