package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.casper.common.error.V2ErrorCode;
import com.oracle.pic.casper.common.exceptions.IfMatchException;
import com.oracle.pic.casper.common.exceptions.IfNoneMatchException;
import com.oracle.pic.casper.common.util.EtagMatchValidation;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Helper methods for working with ETag, If-Match and If-None-Match headers.
 */
public final class HttpMatchHelpers {
    private HttpMatchHelpers() {

    }

    /**
     * Enumeration used by {@link #validateConditionalHeaders) to determine whether If-Match is allowed.
     *
     * If-Match can be disallowed (NO), allowed without star (YES) or allowed with star (YES_WITH_STAR).
     *
     * Swift details:
     * Swift doesn't have any API calls that disallow stars; wildcards are always accepted and always match.  So, our
     * swift implementation does not use the YES enum constant.
     */
    public enum IfMatchAllowed {
        NO,
        YES,
        YES_WITH_STAR
    }

    /**
     * Enumeration used by {@link #validateConditionalHeaders) to determine whether If-None-Match is allowed.
     *
     * If-None-Match can be disallowed (NO), allowed without star (YES), allowed with only a star (STAR_ONLY) or allowed
     * with star (YES_WITH_STAR).
     *
     * Swift details:
     * Swift doesn't have any API calls that disallow stars; wildcards are always accepted and always match.  So, our
     * swift implementation does not use the YES enum constant.
     */
    public enum IfNoneMatchAllowed {
        NO,
        STAR_ONLY,
        YES,
        YES_WITH_STAR
    }

    /**
     * Validate any existing If-Match and If-None-Match headers.
     *
     * Swift details:
     * The only time Swift returns 400 for an OCC header is when you pass a non-wildcard value for PUT If-None-Match,
     * but we're going to be a bit more restrictive.  We return 400 when clients pass in both If-Match and If-None-Match
     * at the same time, and we also return 400 in a few cases where Swift simply ignores OCC headers (i.e. PUT
     * If-Match).
     * Swift doesn't have any API calls that disallow stars; wildcards are always accepted and always match.  So, our
     * swift implementation does not use the YES enum constants and those blocks of this method are unused.
     *
     * @param request the HTTP request to use when fetching the header values.
     * @param ifMatchAllowed NO to disallow If-Match, YES to allow it without "*", YES_WITH_STAR to allow with "*".
     * @param ifNoneMatchAllowed same interpretation as ifMatchAllowed but for If-None-Match.
     */
    public static void validateConditionalHeaders(
            HttpServerRequest request, IfMatchAllowed ifMatchAllowed, IfNoneMatchAllowed ifNoneMatchAllowed) {
        final String ifMatch = request.getHeader(HttpHeaders.IF_MATCH);
        final String ifNoneMatch = request.getHeader(HttpHeaders.IF_NONE_MATCH);
        validateConditionalHeaders(request, ifMatch, ifNoneMatch, ifMatchAllowed, ifNoneMatchAllowed);
    }

    /**
     * Validate any existing If-Match and If-None-Match headers.
     *
     * Swift details:
     * The only time Swift returns 400 for an OCC header is when you pass a non-wildcard value for PUT If-None-Match,
     * but we're going to be a bit more restrictive.  We return 400 when clients pass in both If-Match and If-None-Match
     * at the same time, and we also return 400 in a few cases where Swift simply ignores OCC headers (i.e. PUT
     * If-Match).
     * Swift doesn't have any API calls that disallow stars; wildcards are always accepted and always match.  So, our
     * swift implementation does not use the YES enum constants and those blocks of this method are unused.
     *
     * @param headers the HTTP headers
     * @param ifMatchAllowed NO to disallow If-Match, YES to allow it without "*", YES_WITH_STAR to allow with "*".
     * @param ifNoneMatchAllowed same interpretation as ifMatchAllowed but for If-None-Match.
     */
    public static void validateConditionalHeaders(
            javax.ws.rs.core.HttpHeaders headers, IfMatchAllowed ifMatchAllowed, IfNoneMatchAllowed ifNoneMatchAllowed, String path) {
        final String ifMatch = headers.getHeaderString(HttpHeaders.IF_MATCH.toString());
        final String ifNoneMatch = headers.getHeaderString(HttpHeaders.IF_NONE_MATCH.toString());
        validateConditionalHeaders(headers, ifMatch, ifNoneMatch, ifMatchAllowed, ifNoneMatchAllowed, path);
    }


    public static void validateConditionalHeaders(HttpServerRequest request,
                                                  String ifMatch,
                                                  String ifNoneMatch,
                                                  IfMatchAllowed ifMatchAllowed,
                                                  IfNoneMatchAllowed ifNoneMatchAllowed) {
        if (ifMatch != null && ifNoneMatch != null) {
            throw new HttpException(V2ErrorCode.INVALID_CONDITIONAL_HEADERS,
                "Requests must not contain both If-Match and If-None-Match", request.path());
        }

        if (ifMatchAllowed == IfMatchAllowed.NO && ifMatch != null) {
            throw new HttpException(V2ErrorCode.IF_MATCH_DISALLOWED,
                "This request does not permit the use of the If-Match header", request.path());
        }

        if (ifMatchAllowed == IfMatchAllowed.YES && ifMatch != null && ifMatch.equals("*")) {
            throw new HttpException(V2ErrorCode.IF_MATCH_DISALLOWED,
                "This request does not permit the If-Match header with value '*'", request.path());
        }

        if (ifMatch != null && ifMatch.contains(",")) {
            throw new HttpException(V2ErrorCode.NO_COND_MULT_ENTITIES,
                "This request does not allow multiple entity tags in the If-Match header", request.path());
        }

        if (ifNoneMatchAllowed == IfNoneMatchAllowed.NO && ifNoneMatch != null) {
            throw new HttpException(V2ErrorCode.IF_NONE_MATCH_DISALLOWED,
                "This request does not permit the use of the If-None-Match header", request.path());
        }

        if (ifNoneMatchAllowed == IfNoneMatchAllowed.STAR_ONLY && ifNoneMatch != null && !ifNoneMatch.equals("*")) {
            throw new HttpException(V2ErrorCode.IF_NONE_MATCH_DISALLOWED,
                "This request only allows the If-None-Match header to have the value '*'", request.path());
        }

        if (ifNoneMatchAllowed == IfNoneMatchAllowed.YES && ifNoneMatch != null && ifNoneMatch.equals("*")) {
            throw new HttpException(V2ErrorCode.IF_NONE_MATCH_DISALLOWED,
                "This request does not permit the If-None-Match header with value '*'", request.path());
        }

        if (ifNoneMatch != null && ifNoneMatch.contains(",")) {
            throw new HttpException(V2ErrorCode.NO_COND_MULT_ENTITIES,
                "This request does not allow multiple entity tags in the If-None-Match header", request.path());
        }
    }

    public static void validateConditionalHeaders(javax.ws.rs.core.HttpHeaders headers,
                                                  String ifMatch,
                                                  String ifNoneMatch,
                                                  IfMatchAllowed ifMatchAllowed,
                                                  IfNoneMatchAllowed ifNoneMatchAllowed,
                                                  String path) {
        if (ifMatch != null && ifNoneMatch != null) {
            throw new HttpException(V2ErrorCode.INVALID_CONDITIONAL_HEADERS,
                    "Requests must not contain both If-Match and If-None-Match", path);
        }

        if (ifMatchAllowed == IfMatchAllowed.NO && ifMatch != null) {
            throw new HttpException(V2ErrorCode.IF_MATCH_DISALLOWED,
                    "This request does not permit the use of the If-Match header", path);
        }

        if (ifMatchAllowed == IfMatchAllowed.YES && ifMatch != null && ifMatch.equals("*")) {
            throw new HttpException(V2ErrorCode.IF_MATCH_DISALLOWED,
                    "This request does not permit the If-Match header with value '*'", path);
        }

        if (ifMatch != null && ifMatch.contains(",")) {
            throw new HttpException(V2ErrorCode.NO_COND_MULT_ENTITIES,
                    "This request does not allow multiple entity tags in the If-Match header", path);
        }

        if (ifNoneMatchAllowed == IfNoneMatchAllowed.NO && ifNoneMatch != null) {
            throw new HttpException(V2ErrorCode.IF_NONE_MATCH_DISALLOWED,
                    "This request does not permit the use of the If-None-Match header", path);
        }

        if (ifNoneMatchAllowed == IfNoneMatchAllowed.STAR_ONLY && ifNoneMatch != null && !ifNoneMatch.equals("*")) {
            throw new HttpException(V2ErrorCode.IF_NONE_MATCH_DISALLOWED,
                    "This request only allows the If-None-Match header to have the value '*'", path);
        }

        if (ifNoneMatchAllowed == IfNoneMatchAllowed.YES && ifNoneMatch != null && ifNoneMatch.equals("*")) {
            throw new HttpException(V2ErrorCode.IF_NONE_MATCH_DISALLOWED,
                    "This request does not permit the If-None-Match header with value '*'", path);
        }

        if (ifNoneMatch != null && ifNoneMatch.contains(",")) {
            throw new HttpException(V2ErrorCode.NO_COND_MULT_ENTITIES,
                    "This request does not allow multiple entity tags in the If-None-Match header", path);
        }
    }

    /**
     * Check any conditional headers (If-Match or If-None-Match) against the entity tag and take the appropriate action.
     *
     * An HttpException with a 412 status is thrown in the following cases:
     *
     *  - If-Match is present and not "*", the existing entity tag is present and they do not match each other.
     *  - If-Match is present, the request is a PUT or POST and there is no existing entity (note that this
     *    case is a 404 for GET and HEAD requests, and isn't handled here).
     *  - If-None-Match is "*", the request is a PUT or POST, and there is an existing entity.
     *
     * To handle 304 Not Modified, the method returns true in the following case:
     *
     *  - If-None-Match is present, the existing entity tag is present, the request is a GET or HEAD, and the header and
     *    tag match each other.
     *
     * Note that the return value is only relevant for GET and HEAD requests. All other request methods can ignore the
     * return value, as it will always be false (or an exception is thrown).
     *
     * This method assumes that
     * {@link #validateConditionalHeaders(HttpServerRequest, IfMatchAllowed, IfNoneMatchAllowed)} has been called to
     * validate the conditional headers. It also assumes the following has been validated before this method is called
     * (some of these are validated by validateConditionalHeaders):
     *
     *  - GET and HEAD requests will not call this method with a null etag (this is a 404 in those cases).
     *  - PUT requests, and POST requests that update resources, do not allow If-None-Match to be anything but '*'.
     *  - POST requests to collections (for object creation) do not allow If-Match or If-None-Match headers.
     *  - DELETE requests do not allow If-None-Match headers.
     *
     * Swift details:
     * Swift ignores the If-Match header on object PUTs, allowing them to pass through as if they had no headers.  We
     * return 400 from {@link #validateConditionalHeaders(HttpServerRequest, IfMatchAllowed, IfNoneMatchAllowed)}
     * though, so this method is never called by our swift implementation on a PUT request with an If-Match header.
     * Swift also throws 404s before checking OCC headers on object POSTs (as will we, but we don't currently support
     * object POSTs).
     *
     * @param request the HTTP request to use when fetching the conditional tags.
     * @param etag the entity tag for the fetched entity of this request. For GET and HEAD requests this must not be
     *             null, but it may be null for PUT, POST and DELETE requests (as there may be no existing entity).
     * @return true if the caller should return 304, false otherwise.
     */
    public static boolean checkConditionalHeaders(HttpServerRequest request, @Nullable String etag) {
        final String ifMatch = request.getHeader(HttpHeaders.IF_MATCH);
        final String ifNoneMatch = request.getHeader(HttpHeaders.IF_NONE_MATCH);

        final boolean isPutOrPost = (request.method() == HttpMethod.PUT || request.method() == HttpMethod.POST);
        if (isPutOrPost) {
            try {
                EtagMatchValidation.checkETagConditions(ifMatch, ifNoneMatch, etag);
            } catch (IfMatchException e) {
                throw new HttpException(V2ErrorCode.IF_MATCH_FAILED, request.path(), e);
            } catch (IfNoneMatchException e) {
                throw new HttpException(V2ErrorCode.IF_NONE_MATCH_FAILED, request.path(), e);
            }
            return false;
        }

        final boolean isGetOrHead = (request.method() == HttpMethod.GET || request.method() == HttpMethod.HEAD);
        if (ifMatch != null) {
            if (!ifMatch.equals("*") && etag != null && !ifMatch.equals(etag)) {
                throw new HttpException(V2ErrorCode.IF_MATCH_FAILED,
                    "The If-Match header is '" + ifMatch + "' but the current entity tag is '" + etag + "'",
                    request.path());
            }
        } else if (ifNoneMatch != null) {
            if (isGetOrHead && ("*".equals(ifNoneMatch) || (etag != null && ifNoneMatch.equals(etag)))) {
                return true;
            }
        }

        return false;
    }

    /**
     * Get the value of the If-Match header from an HTTP request.
     *
     * @param request the HTTP request.
     * @return the value of the If-Match header, or null if there is no If-Match header, or if the value was "*". In
     *         this case, we assume that "If-Match: *" matches anything, so it is the same as having no header.
     */
    @Nullable
    public static String getIfMatchHeader(HttpServerRequest request) {
        return Optional.ofNullable(request.getHeader(HttpHeaders.IF_MATCH)).filter(h -> !h.equals("*")).orElse(null);
    }

    /**
     * Validation of new object If-None-Match ETag of new object.
     *
     * @param newObjIfNoneMatchETag: the IfNoneMatch ETag of new object name
     * @return the object name is valid
     *
     */
    public static String validateIfNoneMatchEtag(String newObjIfNoneMatchETag) {
        if (newObjIfNoneMatchETag != null && newObjIfNoneMatchETag != "" && !newObjIfNoneMatchETag.equals("*")) {
            throw new IfNoneMatchException("New object if-none-match etag must either be null or '*'.");
        }
        return newObjIfNoneMatchETag;
    }
}
