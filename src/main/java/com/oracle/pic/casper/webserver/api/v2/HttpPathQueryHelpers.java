package com.oracle.pic.casper.webserver.api.v2;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.oracle.pic.casper.common.config.MultipartLimit;
import com.oracle.pic.casper.common.encryption.Obfuscate;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.model.BucketMeterFlag;
import com.oracle.pic.casper.objectmeta.Api;
import com.oracle.pic.casper.objectmeta.input.BucketSortBy;
import com.oracle.pic.casper.objectmeta.input.BucketSortOrder;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.common.NamespaceCaseWhiteList;
import com.oracle.pic.casper.webserver.api.model.BucketProperties;
import com.oracle.pic.casper.webserver.api.model.ObjectProperties;
import com.oracle.pic.casper.webserver.api.model.exceptions.InvalidPropertyException;
import com.oracle.pic.casper.webserver.api.pars.BackendParId;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import io.vertx.core.http.HttpServerRequest;
import org.apache.commons.codec.binary.Base64;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper methods for dealing with HTTP path and query parameters.
 */
public final class HttpPathQueryHelpers {

    private HttpPathQueryHelpers() {

    }

    static final String PAGE_PARAM = "page";
    static final String LIMIT_PARAM = "limit";
    static final String COMPARTMENT_ID_PARAM = "compartmentId";
    static final String TENANT_ID_PARAM = "tenantId";
    static final String BUCKET_NAME_QUERY_PARAM = "bucketName";
    static final String WORK_REQUEST_TYPE = "workRequestType";
    static final String AVAILABILITY_DOMAIN_PARAM = "availabilityDomain";

    static final int MIN_LIMIT = 1;
    static final int MAX_LIMIT = 1000;

    private static final String PAR_PATH_PREFIX = "/p/";
    private static final String OBJECT_PAR_ACCESS_URI_REGEX = CasperApiV2.PAR_OBJECT_ROUTE;
    private static final String BUCKET_PAR_ACCESS_URI_REGEX = "/p/([^/]+)/n/([^/]+)/b/([^/]+)/o/?";

    /**
     * Get a namespace name from an HTTP request URL path.
     * Lower cases the namespace to support case insensitive lookups.
     *
     * @param request the HTTP request from which to get the namespace path parameter.
     * @return the extracted but not-yet-validated namespace name.
     *
     * @see {@link CasperApiV2} for details on URL path format.
     */
    public static String getNamespaceName(HttpServerRequest request, WSRequestContext context) {
        String param = isParPath(request) ? CasperApiV2.PARAM_1 : CasperApiV2.PARAM_0;
        String namespace = getRequiredParam(request, param, V2ErrorCode.INTERNAL_SERVER_ERROR, "internal server error");
        namespace = NamespaceCaseWhiteList.lowercaseNamespace(Api.V2, namespace);
        context.setNamespaceName(namespace);
        return namespace;
    }

    /**
     * Get the bucket name from the HTTP request URL path and update the request context to include it.
     */
    public static String getBucketName(HttpServerRequest request, WSRequestContext context) {
        final String param = isParPath(request) ? CasperApiV2.PARAM_2 : CasperApiV2.PARAM_1;
        final String bucketName = getRequiredParam(request, param, V2ErrorCode.INTERNAL_SERVER_ERROR,
                "internal server error");
        context.setBucketName(bucketName);
        return bucketName;
    }

    /**
     * Get an object name from an HTTP request URL path
     *
     * @param request the HTTP request from which to get the object name path parameter.
     * @return the extracted but not-yet-validated object name.
     *
     * @see {@link CasperApiV2} for details on the URL path format.
     */
    public static String getObjectName(HttpServerRequest request, WSRequestContext context) {
        String param = isParPath(request) ? CasperApiV2.PARAM_3 : CasperApiV2.PARAM_2;
        String objectName = getRequiredParam(request, param, V2ErrorCode.INTERNAL_SERVER_ERROR,
                "internal server error");
        context.setObjectName(objectName);
        return objectName;
    }

    /**
     * Returns true if the request is a PAR request containing a nonce.
     *
     * If the request contains a nonce at the beginning, then the namespace, bucket, and object name parameters will
     * all have their path param index incremented by one.
     *
     * @see {@link CasperApiV2} for details on the URL path format.
     */
    private static boolean isParPath(HttpServerRequest request) {
        return request.path().startsWith(PAR_PATH_PREFIX);
    }

    public static String getUploadId(HttpServerRequest request) {
        String param = isParPath(request) ? CasperApiV2.PARAM_4 : CasperApiV2.UPLOAD_PARAM;
        return getRequiredParam(request, param, V2ErrorCode.MISSING_UPLOAD_ID,
                "The upload ID is missing.");
    }

    public static String getParPathNonce(HttpServerRequest request) {
        String nonce = getRequiredParam(request, CasperApiV2.PARAM_0,
                V2ErrorCode.INTERNAL_SERVER_ERROR, "internal server error");
        if (!Base64.isBase64(nonce)) {
            throw new HttpException(V2ErrorCode.INVALID_PAR_URI,
                    "Invalid Pre-Authenticated Request access URI. This should be in the response of creating the " +
                            "Pre-Authenticated Request, and start with '/p/'",
                    request.path());
        }
        return nonce;
    }

    public static String getReplicationPolicyId(HttpServerRequest request) {
        return getRequiredParam(request, CasperApiV2.PARAM_2, V2ErrorCode.INVALID_REPLICATION_POLICY_ID,
                "Missing Replication Policy Id");
    }

    private static String getRequiredParam(HttpServerRequest request, String paramName,
                                           V2ErrorCode errorCode, String message) {
        String param = request.getParam(paramName);
        if (param == null || param.isEmpty()) {
            throw new HttpException(errorCode, message, request.path());
        }

        return param;
    }

    public static int getUploadPartNumParam(HttpServerRequest request) {
        String param = isParPath(request) ? CasperApiV2.PARAM_5 : CasperApiV2.UPLOAD_PART_NUMBER_PARAM;
        String uploadPartNum = request.getParam(param);
        final int parsedPartNum;
        try {
            parsedPartNum = Integer.parseInt(uploadPartNum);
        } catch (NumberFormatException nfex) {
            throw new HttpException(V2ErrorCode.INVALID_UPLOAD_PART_NUMBER,
                "The " + CasperApiV2.UPLOAD_PART_NUMBER_PARAM + " query parameter must be a valid integer (it was " +
                    uploadPartNum + ")", request.path());
        }
        if (!MultipartLimit.isWithinRange(parsedPartNum)) {
            throw new HttpException(V2ErrorCode.INVALID_UPLOAD_PART_NUMBER, MultipartLimit.PART_NUM_ERROR_MESSAGE,
                request.path());
        }
        return parsedPartNum;
    }

    public static Optional<String> getStartWith(HttpServerRequest request) {
        String sa = request.getParam(CasperApiV2.START_WITH_PARAM);
        if (Strings.isNullOrEmpty(sa)) {
            return Optional.empty();
        }

        return Optional.of(sa);
    }

    public static Optional<String> getEndBefore(HttpServerRequest request) {
        String ew = request.getParam(CasperApiV2.END_BEFORE_PARAM);

        return Optional.ofNullable(ew);
    }

    public static Optional<String> getPrefix(HttpServerRequest request) {
        String pf = request.getParam(CasperApiV2.PREFIX_PARAM);

        return Optional.ofNullable(pf);
    }

    public static Optional<String> getObjectNamePrefix(HttpServerRequest request) {
        String pf = request.getParam(CasperApiV2.OBJECT_NAME_PREFIX_PARAM);

        return Optional.ofNullable(pf);
    }

    public static Optional<String> getBucketNameQueryParam(HttpServerRequest request) {
        String pf = request.getParam(BUCKET_NAME_QUERY_PARAM);

        return Optional.ofNullable(pf);
    }

    /**
     * Extract parUrl from a List PARs request (when looking up a PAR)
     *
     * From a product perspective, a user might be mindful and give us the parUrl starting with /p/, but he/she could
     * also just paste the entire URI with scheme and prefix, i.e. https://objectstorage.<region>.oraclecloud.com/p/...
     * We support both cases.
     *
     * Also, for bucket-level PARs, we allow parUrls both with a trailing slash (/o/) and without a trailing slash (/o).
     */
    public static Optional<BackendParId> getParIdFromParUrl(HttpServerRequest request) {
        final String parUrlParam = request.getParam(CasperApiV2.PAR_URL_PARAM);
        if (Strings.isNullOrEmpty(parUrlParam)) {
            return Optional.empty();
        }
        final String message = "Invalid Pre-Authenticated Request access URI. This should be in the response of " +
                "creating the Pre-Authenticated Request, and start with '/p/'";
        String parUrl;
        try {
            parUrl = new URI(parUrlParam).getPath();
        } catch (URISyntaxException urise) {
            throw new HttpException(V2ErrorCode.INVALID_PAR_URI, message, request.path());
        }
        if (parUrl == null) {
            throw new HttpException(V2ErrorCode.INVALID_PAR_URI, message, request.path());
        }
        // try to parse components of PAR out of parUrl
        String nonce;
        String namespace;
        String bucketName;
        String objectName = null;
        // first try object PAR regex, then try bucket PAR regex
        final Matcher objectAccessUriMatcher = Pattern.compile(OBJECT_PAR_ACCESS_URI_REGEX).matcher(parUrl);
        final Matcher bucketAccessUriMatcher = Pattern.compile(BUCKET_PAR_ACCESS_URI_REGEX).matcher(parUrl);
        if (objectAccessUriMatcher.matches()) {
            nonce = objectAccessUriMatcher.group(1);
            namespace = objectAccessUriMatcher.group(2);
            bucketName = objectAccessUriMatcher.group(3);
            objectName = objectAccessUriMatcher.group(4);
        } else if (bucketAccessUriMatcher.matches()) {
            nonce = bucketAccessUriMatcher.group(1);
            namespace = bucketAccessUriMatcher.group(2);
            bucketName = bucketAccessUriMatcher.group(3);
        } else {
            throw new HttpException(V2ErrorCode.INVALID_PAR_URI, message, request.path());
        }

        return Optional.of(new BackendParId(namespace, bucketName, objectName, nonce));
    }

    public static ImmutableList<ObjectProperties> getRequestedObjectParametersForList(HttpServerRequest request) {
        String requestedList = request.getParam(CasperApiV2.FIELDS_PARAM);
        if (requestedList == null) {
            return ImmutableList.of(ObjectProperties.NAME);
        }
        try {
            ImmutableList<ObjectProperties> list = ObjectProperties.parseConcatenatedList(requestedList);
            if (!list.contains(ObjectProperties.NAME)) {
                // always include name
                return ImmutableList.<ObjectProperties>builder().addAll(list).add(ObjectProperties.NAME).build();
            } else {
                return list;
            }
        } catch (InvalidPropertyException e) {
            throw new HttpException(V2ErrorCode.INVALID_OBJECT_PROPERTY,
                "Requested object fields '" + requestedList + "' is invalid", request.path(), e);
        }
    }


    public static Character getDelimiter(HttpServerRequest request) {
        String dl = request.getParam(CasperApiV2.DELIMITER_PARAM);
        if (dl == null) return null;  //defaults
        if (!"/".equals(dl)) throw new HttpException(V2ErrorCode.INVALID_OBJECT_NAME,
            "The delimiter parameter '" + dl + "' is not supported", request.path());
        return '/';   // FIXME only '/' is supported for now
    }

    /**
     * Get the "page" parameter from the query string of an HTTP request.
     * Page parameter is something that was provided in OPC_NEXT_PAGE header
     * So it has to be a valid obfuscated string
     *
     * @param request the HTTP request from which to get the parameter.
     * @return the value of the parameter, which may be null if the paramter is missing.
     */
    public static Optional<String> getPage(HttpServerRequest request) {
        try {
            return Optional.ofNullable(Obfuscate.deObfuscate(request.getParam(PAGE_PARAM)));
        } catch (IllegalArgumentException ex) {
            throw new HttpException(V2ErrorCode.INVALID_PAGE,
                "The page '" + request.getParam(PAGE_PARAM) + "' is not valid", request.path(), ex);
        }
    }

    public static Optional<String> getBucketsPage(HttpServerRequest request) {
        return getPage(request);
    }

    @Nullable
    public static Integer getUploadPartPage(HttpServerRequest request) {
        final Optional<String> page = getPage(request);
        if (!page.isPresent()) {
            return null;
        } else {
            int part;
            try {
                part = Integer.parseInt(page.get());
            } catch (NumberFormatException e) {
                throw new HttpException(V2ErrorCode.INVALID_PAGE,
                    "The page '" + request.getParam(PAGE_PARAM) + "' is not a valid integer", request.path(), e);
            }
            if (!MultipartLimit.isWithinRange(part)) {
                throw new HttpException(V2ErrorCode.INVALID_PAGE, MultipartLimit.PART_NUM_ERROR_MESSAGE,
                    request.path());
            }
            return part;
        }
    }

    public static OptionalInt getLimit(HttpServerRequest request) {
        String limitStr = request.getParam(LIMIT_PARAM);
        if (limitStr == null) {
            return OptionalInt.empty();
        }

        final int value;
        try {
            value = Integer.parseInt(limitStr);
        } catch (NumberFormatException nfex) {
            throw new HttpException(V2ErrorCode.INVALID_LIMIT,
                "The 'limit' query parameter must be a valid integer (it was '" + limitStr + "')", request.path(),
                nfex);
        }

        if (value < MIN_LIMIT || value > MAX_LIMIT) {
            throw new HttpException(V2ErrorCode.INVALID_LIMIT,
                "The 'limit' query parameter must be between " + MIN_LIMIT + " and " + MAX_LIMIT +
                    " (it was " + value + ")", request.path());
        }

        return OptionalInt.of(value);
    }

    public static Optional<String> getCompartmentId(HttpServerRequest request) {
        return Optional.ofNullable(request.getParam(COMPARTMENT_ID_PARAM));
    }

    public static Optional<String> getTenantId(HttpServerRequest request) {
        return Optional.ofNullable(request.getParam(TENANT_ID_PARAM));
    }

    public static Optional<String> getAd(HttpServerRequest request) {
        return Optional.ofNullable(request.getParam(AVAILABILITY_DOMAIN_PARAM));
    }

    public static Optional<String> getWorkRequestType(HttpServerRequest request) {
        return Optional.ofNullable(request.getParam(WORK_REQUEST_TYPE));
    }

    public static Set<BucketProperties> getRequestedBucketParametersForList(HttpServerRequest request,
                                                                            BucketProperties... props) {
        String requestedList = request.getParam(CasperApiV2.FIELDS_PARAM);
        if (requestedList == null) {
            return Collections.emptySet();
        }
        try {
            return BucketProperties.parseConcatenatedList(requestedList, props);
        } catch (InvalidPropertyException e) {
            throw new HttpException(V2ErrorCode.INVALID_BUCKET_PROPERTY,
                "Requested bucket fields '" + requestedList + "' is invalid", request.path(), e);
        }
    }

    public static Optional<BucketSortBy> getSortBy(HttpServerRequest request) {
        String sortBy = request.getParam(CasperApiV2.SORT_BY_PARAM);
        if (Strings.isNullOrEmpty(sortBy)) {
            return Optional.empty();
        }
        try {
            return Optional.of(BucketSortBy.fromValue(sortBy));
        } catch (IllegalArgumentException e) {
            throw new HttpException(V2ErrorCode.INVALID_SORT_BY,
                "The 'sortBy' query parameter must be one of " + Arrays.toString(BucketSortBy.values()) +
                    " (it was '" + sortBy + "')", request.path(), e);
        }
    }

    public static Optional<BucketSortOrder> getSortOrder(HttpServerRequest request) {
        String sortOrder = request.getParam(CasperApiV2.SORT_ORDER_PARAM);
        if (Strings.isNullOrEmpty(sortOrder)) {
            return Optional.empty();
        }
        try {
            return Optional.of(BucketSortOrder.fromValue(sortOrder));
        } catch (IllegalArgumentException e) {
            throw new HttpException(V2ErrorCode.INVALID_SORT_ORDER,
                "The 'sortOrder' query parameter must be one of " +
                    Arrays.toString(BucketSortOrder.values()) + " (it was '" + sortOrder + "')", request.path(), e);
        }
    }

    public static String getWorkRequestId(HttpServerRequest request) {
        return getRequiredParam(request, CasperApiV2.PARAM_0,
                V2ErrorCode.INTERNAL_SERVER_ERROR, "internal server error");
    }

    public static Optional<BucketMeterFlag> getMeterSetting(HttpServerRequest request) {
        String meterSetting = request.getParam(CasperApiV2.METER_SETTING_PARAM);
        if (Strings.isNullOrEmpty(meterSetting)) {
            return Optional.empty();
        }
        try {
            return Optional.of(BucketMeterFlag.fromValue(meterSetting));
        } catch (IllegalArgumentException e) {
            throw new HttpException(V2ErrorCode.INVALID_METER_SETTING,
                    "The 'meterSetting' query parameter must be a predefined setting name.",
                    request.path(), e);
        }
    }

    // Get the limit name for getUsage api in the http path.
    public static Optional<String> getLimitParam(HttpServerRequest request) {
        String limitName =
            getRequiredParam(request, CasperApiV2.PARAM_0, V2ErrorCode.INTERNAL_SERVER_ERROR, "internal server error");
        if (Strings.isNullOrEmpty(limitName)) {
            return Optional.empty();
        }
        return Optional.of(limitName.toLowerCase());
    }
}
