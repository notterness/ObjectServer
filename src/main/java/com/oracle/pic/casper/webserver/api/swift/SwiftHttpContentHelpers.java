package com.oracle.pic.casper.webserver.api.swift;

import com.google.common.base.CharMatcher;
import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.model.ArchivalState;
import com.oracle.pic.casper.common.model.Pair;
import com.oracle.pic.casper.common.rest.ContentType;
import com.oracle.pic.casper.webserver.api.common.HttpContentHelpers;
import com.oracle.pic.casper.webserver.api.common.HttpException;
import com.oracle.pic.casper.webserver.api.model.ObjectMetadata;
import com.oracle.pic.casper.webserver.api.model.swift.ContainerAndObject;
import com.oracle.pic.casper.webserver.api.model.swift.SwiftBulkDeleteResponse;
import com.oracle.pic.casper.webserver.util.Validator;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public final class SwiftHttpContentHelpers {

    public static final String FORMAT_PARAM = "format";
    public static final String XML_FORMAT = "xml";

    private SwiftHttpContentHelpers() {
    }

    /**
     * Perform simple HTTP content negotiation for storage object content in an HTTP request.
     *
     * This method does the following:
     *
     *  1. The Content-Type is ignored (see {@link PutObjectHandler} for the default value if none is
     *     provided by the client).
     *  2. If the Content-Length is not provided a 411 (Length Required) error is thrown.
     *  3. If the Content-Length is too large a 413 (Payload Too Large) error is thrown.
     *  4. If the Content-Length is set to something that isn't a positive number a 400 (Bad Request) error is thrown.
     *
     * Swift details:
     * This method differs from its counterpart in {@link HttpContentHelpers}
     * only in error strings.  Not necessarily worth preserving the difference...
     */
    public static void negotiateStorageObjectContent(HttpServerRequest request, long maxSizeBytes) {
        String contentLenStr = request.getHeader(HttpHeaders.CONTENT_LENGTH);
        if (contentLenStr == null) {
            throw new HttpException(V2ErrorCode.CONTENT_LEN_REQUIRED, "Missing Content-Length header.", request.path());
        }

        try {
            long contentLen = Long.parseLong(contentLenStr);
            if (contentLen > maxSizeBytes) {
                throw new HttpException(V2ErrorCode.ENTITY_TOO_LARGE, "Your request is too large.", request.path());
            }

            if (contentLen < 0) {
                throw new HttpException(V2ErrorCode.INVALID_CONTENT_LEN, "Invalid Content-Length", request.path());
            }
        } catch (NumberFormatException nfex) {
            throw new HttpException(V2ErrorCode.INVALID_CONTENT_LEN, "", request.path(), nfex);
        }
    }

    /**
     * Return the format that should be used to respond to this request.
     *
     * Swift details:
     *
     * Swift accepts a couple different format arguments from clients:  the Accept header and the "format" query param.
     * The Accept header can be "application/json", "application/xml", "text/xml", or "text/plain", and the format
     * query param can be "json" or "xml".  Swift defaults to plain text, ignores case on query param values, and
     * allows them to override the Accept header.  Swift ignores unrecognized format query param values, and returns
     * 406 Not Acceptable for unrecognized Accept headers.
     * Our Swift implementation doesn't currently support the plain text format.  We follow Swift's example, except
     * that we'll also return 406 for "Accept: text/plain", and we default to JSON.
     *
     * Note:  if you're adding support for plain text, ensure that all browsers are safe from XSS scripting attacks.
     * See https://jira.oci.oraclecorp.com/browse/CASPER-448 for an example.  If an attacker can get a user to visit a
     * page that contains tenant names, bucket names, or object names that they control, then the attacker can get the
     * user to load an HTML {@code <script>} tag.  Browsers shouldn't execute script tags on documents with content
     * type "text/plain", but check and make sure.  If the browser executes the script, you've got a XSS vulnerability
     * and you'll need HTML escaping.
     */
    // TODO(jfriedly):  See if using common's HttpServerUtil.getAcceptContent() might work better here.
    // If so, fix up the OpcInstaller tests in SwiftHttpContentHelpersTest
    public static SwiftResponseFormat readResponseFormat(HttpServerRequest request) {
        SwiftResponseFormat format = SwiftResponseFormat.JSON;
        final String acceptHeader = request.getHeader(HttpHeaders.ACCEPT);
        final String formatParam = request.getParam(FORMAT_PARAM);
        if (acceptHeader != null) {
            switch (acceptHeader) {
                case ContentType.APPLICATION_JSON:
                case ContentType.WILDCARD:
                    format = SwiftResponseFormat.JSON;
                    break;
                case ContentType.APPLICATION_XML:
                case ContentType.TEXT_XML:
                    format = SwiftResponseFormat.XML;
                    break;
                default:
                    // My "real Swift" test setup throws errors here, but OPC Swift doesn't.  To ensure that the OPC
                    // installer can run, we err on the side of permissiveness.
                    break;
            }
        }

        // format query param overrides Accept header
        if (formatParam != null) {
            if (XML_FORMAT.equalsIgnoreCase(formatParam)) {
                format = SwiftResponseFormat.XML;
            } else {
                // Do *not* throw error for unsupported format query param, return to default (JSON)
                format = SwiftResponseFormat.JSON;
            }
        }

        return format;
    }

    public static ObjectMetadata readObjectMetadata(HttpServerRequest request,
                                                    String accountName,
                                                    String containerName,
                                                    String objectName) {

        final Map<String, String> metadata = SwiftHttpHeaderHelpers.getObjectMetadataHeaders(request);
        final long contentLen = Long.parseLong(request.getHeader(HttpHeaders.CONTENT_LENGTH));

        return new ObjectMetadata(
                accountName,
                containerName,
                objectName,
                contentLen,
                null,
                metadata,
                DateUtils.addHours(new Date(), 24),
                new Date(),
                null,
                null,
                null,
                ArchivalState.Available);
    }

    public static Pair<List<ContainerAndObject>, List<SwiftBulkDeleteResponse.ErrorObject>> readContainerAndObjectList(
        HttpServerRequest request, byte[] bytes, int limit) {

        final String bytesAsString;
        try {
            bytesAsString = IOUtils.toString(bytes, "UTF-8");
        } catch (IOException e) {
            throw new HttpException(V2ErrorCode.INVALID_CONTENT_TYPE, "Cannot parse request body as UTF-8 text.",
                request.path(), e);
        }

        List<ContainerAndObject> containerAndObjects = new ArrayList<>();
        List<SwiftBulkDeleteResponse.ErrorObject> errors = new ArrayList<>();
        String[] lines = bytesAsString.split("\n");
        if (lines.length > limit) {
            throw new HttpException(V2ErrorCode.ENTITY_TOO_LARGE,
                "The maximum number of objects in one bulk delete is " + limit, request.path());
        }
        for (String line : lines) {
            //quick check for name length
            if (line.length() > Validator.MAX_BUCKET_LENGTH + 1 + Validator.MAX_OBJECT_LENGTH) {
                errors.add(new SwiftBulkDeleteResponse.ErrorObject(line,
                    errorCodeFormat(V2ErrorCode.INVALID_OBJECT_NAME)));
            } else {
                String[] lineSplit = line.split("/", 2);
                if (lineSplit.length == 1) {
                    //deletion of container via bulk delete is not supported in this implementation
                    errors.add(new SwiftBulkDeleteResponse.ErrorObject(line,
                        errorCodeFormat(V2ErrorCode.NOT_IMPLEMENTED)));
                } else if (lineSplit.length == 2) {
                    //check for url encoded names
                    if (!isURLEncoded(lineSplit[0]) || !isURLEncoded(lineSplit[1])) {
                        errors.add(new SwiftBulkDeleteResponse.ErrorObject(line,
                            errorCodeFormat(V2ErrorCode.INVALID_OBJECT_NAME)));
                    } else {
                        final String containerName;
                        final String objectName;
                        try {
                            containerName = URLDecoder.decode(lineSplit[0], StandardCharsets.UTF_8.name());
                        } catch (Exception e) {
                            errors.add(new SwiftBulkDeleteResponse.ErrorObject(line,
                                errorCodeFormat(V2ErrorCode.INVALID_BUCKET_NAME)));
                            continue;
                        }
                        try {
                            objectName = URLDecoder.decode(lineSplit[1], StandardCharsets.UTF_8.name());
                        } catch (Exception e) {
                            errors.add(new SwiftBulkDeleteResponse.ErrorObject(line,
                                errorCodeFormat(V2ErrorCode.INVALID_OBJECT_NAME)));
                            continue;
                        }
                        containerAndObjects.add(new ContainerAndObject(containerName, objectName));
                    }
                } else {
                    errors.add(new SwiftBulkDeleteResponse.ErrorObject(line,
                        errorCodeFormat(V2ErrorCode.INVALID_OBJECT_NAME)));
                }
            }
        }
        return Pair.pair(containerAndObjects, errors);
    }

    static boolean isURLEncoded(String content) {
        return CharMatcher.ASCII.matchesAllOf(content) && !content.contains(" ");
    }

    private static String errorCodeFormat(V2ErrorCode e) {
        return e.getStatusCode() + " " + e.getErrorName();
    }
}
