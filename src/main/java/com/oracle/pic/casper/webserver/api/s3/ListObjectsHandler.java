package com.oracle.pic.casper.webserver.api.s3;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.oracle.pic.casper.common.config.ConfigRegion;
import com.oracle.pic.casper.common.encryption.Obfuscate;
import com.oracle.pic.casper.common.error.S3ErrorCode;
import com.oracle.pic.casper.common.monitoring.WebServerMonitoringMetric;
import com.oracle.pic.casper.webserver.api.auth.AuthenticationInfo;
import com.oracle.pic.casper.webserver.api.auth.CasperOperation;
import com.oracle.pic.casper.webserver.api.auth.S3Authenticator;
import com.oracle.pic.casper.webserver.api.backend.Backend;
import com.oracle.pic.casper.webserver.api.common.MetricsHandler;
import com.oracle.pic.casper.webserver.api.common.SyncHandler;
import com.oracle.pic.casper.webserver.api.model.ObjectProperties;
import com.oracle.pic.casper.webserver.api.model.ObjectSummaryCollection;
import com.oracle.pic.casper.webserver.api.model.serde.ObjectSummary;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3;
import com.oracle.pic.casper.webserver.api.ratelimit.EmbargoV3Operation;
import com.oracle.pic.casper.webserver.api.s3.model.ListBucketResultV1;
import com.oracle.pic.casper.webserver.api.s3.model.ListBucketResultV2;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

/**
 * We skip encoding for V2 because Amazon's S3 does not actually encode these values even if encodingType is
 * specified, despite what their docs say.  We decide to follow their implementation as tools such as boto
 * expects this behaviour.  See JIRA ticket CASPER-1222 for more detail.
 */
public class ListObjectsHandler extends SyncHandler {
    private final S3Authenticator authenticator;
    private final XmlMapper mapper;
    private final Backend backend;
    private final EmbargoV3 embargoV3;
    private static final int DEFAULT_LIMIT = 1000;
    private static final ImmutableList<ObjectProperties> OBJECT_PROPERTIES = ImmutableList.of(ObjectProperties.NAME,
            ObjectProperties.MD5, ObjectProperties.TIMEMODIFIED, ObjectProperties.SIZE,
            ObjectProperties.ARCHIVED);
    public ListObjectsHandler(S3Authenticator authenticator, XmlMapper mapper, Backend backend, EmbargoV3 embargoV3) {
        this.authenticator = authenticator;
        this.mapper = mapper;
        this.backend = backend;
        this.embargoV3 = embargoV3;
    }

    @Override
    public void handleSync(RoutingContext context) {
        MetricsHandler.addMetrics(context, WebServerMetrics.S3_GET_BUCKET_BUNDLE);
        WSRequestContext.setOperationName("s3", getClass(), context, CasperOperation.LIST_OBJECTS);

        final HttpServerRequest request = context.request();
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        final String namespace = S3HttpHelpers.getNamespace(request, wsRequestContext);
        final String bucketName = S3HttpHelpers.getBucketName(request, wsRequestContext);
        final int limit = Math.min(S3HttpHelpers.getMaxKeys(request).orElse(DEFAULT_LIMIT), DEFAULT_LIMIT);
        final String prefix = S3HttpHelpers.getPrefix(request);
        final String delimiter = S3HttpHelpers.getDelimiter(request);
        final String encodingType = S3HttpHelpers.getEncoding(request).orElse(null);

        final EmbargoV3Operation embargoOperation = EmbargoV3Operation.builder()
            .setApi(EmbargoV3Operation.Api.S3)
            .setOperation(CasperOperation.LIST_OBJECTS)
            .setNamespace(namespace)
            .setBucket(bucketName)
            .build();
        embargoV3.enter(embargoOperation);

        final boolean isV2 = S3HttpHelpers.isListV2(request);
        final String marker = isV2 ? null : S3HttpHelpers.getMarker(request);
        final String continuationToken = isV2 ? S3HttpHelpers.getContinuationToken(request) : null;
        final String startAfter = isV2 && continuationToken == null ? S3HttpHelpers.getStartAfter(request) : null;
        final boolean fetchOwner = isV2 ? S3HttpHelpers.getFetchOwner(request) : true;
        final boolean encodeEnabled = encodingType != null;
        wsRequestContext.setWebServerMonitoringMetric(WebServerMonitoringMetric.LISTOBJECT_REQUEST_COUNT);

        final String backendStartWith;

        /*
         * The request can have either the continuation token (V2) or (and) startAfter (V2) or a marker (V1)
         * These tokens work as startAfter and for our backend we need to convert it to "startWith".
         */
        String startAfterToken = null;
        if (continuationToken != null) {
            try {
                startAfterToken = Obfuscate.deObfuscate(continuationToken);
            } catch (IllegalArgumentException ex) {
                throw new S3HttpException(S3ErrorCode.INVALID_ARGUMENT, "Illegal argument: unable to decode the token",
                        context.request().path());
            }
        } else if (startAfter != null && !startAfter.isEmpty()) {
            startAfterToken = startAfter;
        } else if (marker != null && !marker.isEmpty()) {
            startAfterToken = marker;
        }

        //we only support 1 character delimiter
        Character delimitChar = (delimiter != null && delimiter.length() == 1) ? delimiter.charAt(0) : null;

        backendStartWith = KeyUtils.startWith(startAfterToken, prefix, delimitChar);

        S3HttpHelpers.failSigV2(request);
        final String contentSha256 = S3HttpHelpers.getContentSha256(request);
        S3HttpHelpers.validateEmptySha256(contentSha256, request);

        try {
            AuthenticationInfo authInfo = authenticator.authenticate(context, contentSha256);
            final ObjectSummaryCollection summary;
            if (limit == 0) {
                summary = new ObjectSummaryCollection(ImmutableList.of(), bucketName, ImmutableList.of(),
                        (ObjectSummary) null, null, null);
            } else {
                summary = backend.listObjects(context, authInfo,
                        OBJECT_PROPERTIES, limit, namespace, bucketName,
                        delimitChar, prefix, backendStartWith, null);
            }

            ListBucketResultV1 resultV1 = backendToResultV1(summary, authInfo, limit, fetchOwner, encodeEnabled,
                prefix, delimiter, marker, encodingType);
            S3XmlResult result = isV2 ?
                new ListBucketResultV2(resultV1, startAfter, continuationToken,
                Obfuscate.obfuscate(resultV1.getNextMarker())) :
                resultV1;
            context.response().putHeader(S3Api.BUCKET_REGION, ConfigRegion.fromSystemProperty().getFullName());
            S3HttpHelpers.writeXmlResponse(context.response(), result, mapper);
        } catch (Exception ex) {
            throw S3HttpException.rewrite(context, ex);
        }
    }

    /**
     * Form the response body from the backend result from backendListObjects().
     * The backend result may contain the startAfter item; remove if present.
     * The backend result may have 1 more item than the limit; trim off the last item if needed.
     */
    static ListBucketResultV1 backendToResultV1(ObjectSummaryCollection summary,
                                                AuthenticationInfo authInfo,
                                                int limit,
                                                boolean fetchOwner,
                                                boolean encodeEnabled,
                                                @Nullable String prefix,
                                                @Nullable String delimiter,
                                                @Nullable String marker,
                                                @Nullable String encodingType)
        throws Exception {
        //encode the fields if needed
        ListBucketResultV1.Builder builder = new ListBucketResultV1.Builder()
            .maxKeys(limit)
            .prefix(encodeEnabled ? encode(prefix) : prefix)
            .delimiter(encodeEnabled ? encode(delimiter) : delimiter)
            .marker(encodeEnabled ? encode(marker) : marker)
            .encodingType(encodingType);

        //owner info if requested
        Owner owner = fetchOwner ? new Owner(authInfo.getMainPrincipal()) : null;

        String nextStartAfter = null;

        //It is possible that we have gone over the limit by greater than 1.
        // Add objects and prefixes to result one by one.
        int results = 0;
        Queue<ObjectSummary> objectQueue = summary.getObjects() != null ?
                Lists.newLinkedList(Lists.newArrayList(summary.getObjects())) :
                new LinkedList();
        Queue<String> prefixQueue = summary.getPrefixes() != null ?
                Lists.newLinkedList(Lists.newArrayList(summary.getPrefixes())) :
                new LinkedList();

        final List<ListBucketResultV1.Content> contentResult = new ArrayList<>();
        final List<ListBucketResultV1.Prefix> prefixesResult = new ArrayList<>();

        while ((!objectQueue.isEmpty() || !prefixQueue.isEmpty()) && results < limit) {
            if (!objectQueue.isEmpty() &&
                    (prefixQueue.isEmpty() || objectQueue.peek().getName().compareTo(prefixQueue.peek()) < 0)) {
                final ListBucketResultV1.Content content =
                        objectSummaryToContent(objectQueue.remove(), encodeEnabled, owner);
                contentResult.add(content);
                nextStartAfter = content.getKey();
            } else {
                final ListBucketResultV1.Prefix commonPrefix =
                        prefixToCommonPrefix(prefixQueue.remove(), encodeEnabled);
                prefixesResult.add(commonPrefix);
                nextStartAfter = commonPrefix.getPrefix();
            }
            results++;
            assert results > 0;
            assert nextStartAfter != null;

        }

        Preconditions.checkState(contentResult.size() + prefixesResult.size() <= limit);

        builder.name(summary.getBucketName()).commonPrefixes(prefixesResult).contents(contentResult);
        /*
         * The results are truncated if:
         * 1) There are objects left in the object queue or prefixes left in the prefix queue which were not added to the result
         * 2) summary tells us that there are objects left to be given back
         */
        if (!objectQueue.isEmpty() || !prefixQueue.isEmpty() || summary.getNextStartWith() != null) {
            builder.nextMarker(nextStartAfter) //Already encoded
                    .truncated(true);
        } else {
            builder.nextMarker(null).truncated(false);
        }

        return builder.build();
    }

    private static ListBucketResultV1.Content objectSummaryToContent(ObjectSummary summary,
                                                                     boolean encodeEnabled,
                                                                     @Nullable Owner owner)
        throws Exception {
        return new ListBucketResultV1.Content(
            encodeEnabled ? encode(summary.getName()) : S3XmlResponseUtil.sanitizeForXml(summary.getName()),
            DateUtils.truncate(summary.getTimeModified(), Calendar.SECOND),
            Optional.ofNullable(summary.getChecksum()).map(c -> c.getQuotedHex()).orElse(null),
            summary.getSize(),
            S3StorageClass.STANDARD,
            owner);
    }

    private static ListBucketResultV1.Prefix prefixToCommonPrefix(String prefix, boolean encodeEnabled)
            throws Exception {
        return new ListBucketResultV1.Prefix(encodeEnabled ? encode(prefix) :
                S3XmlResponseUtil.sanitizeForXml(prefix));
    }

    private static String encode(@Nullable String unencoded) throws UnsupportedEncodingException {
        return unencoded == null ? null :
            URLEncoder.encode(unencoded, StandardCharsets.UTF_8.name()).replace("%2F", "/");
    }
}
