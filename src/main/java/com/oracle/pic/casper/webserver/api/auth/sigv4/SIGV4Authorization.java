package com.oracle.pic.casper.webserver.api.auth.sigv4;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.oracle.pic.casper.common.exceptions.AuthorizationHeaderMalformedException;
import com.oracle.pic.casper.common.exceptions.AuthorizationQueryParametersException;
import com.oracle.pic.casper.webserver.auth.dataplane.sigv4.SIGV4SigningKeyCalculator;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The information contained in a SIGV4 authorization header. An authorization header looks like this:
 * Authorization: AWS4-HMAC-SHA256 Credential=AKIAI6FEC7AFNXEDNGUA/20170225/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=27e44bb4f43d5e7f5575848a86a63eb45250577bdd9cc8127828b4243b8079f7
 */
public class SIGV4Authorization {

    // Constants used in Authorization generation

    /**
     * Authorization algorithm.
     */
    private static final String AWS4_HMAC_SHA256 = "AWS4-HMAC-SHA256";

    /**
     * Query parameter that identifies the algorithm,
     */
    public static final String X_AMZ_ALGORITHM = "X-Amz-Algorithm";

    /**
     * Query parameter that identifies the presigned request expire time,
     */
    public static final String X_AMZ_EXPIRES = "X-Amz-Expires";

    /**
     * Query parameter that gives the signature.
     */
    public static final String X_AMZ_SIGNATURE = "X-Amz-Signature";

    /**
     * Query parameter that lists the signed headers.
     */
    private static final String X_AMZ_SIGNED_HEADERS = "X-Amz-SignedHeaders";

    /**
     * Query parameter that contains the credential.
     */
    private static final String X_AMZ_CREDENTIAL = "X-Amz-Credential";

    private static final String AWS4_REQUEST = "aws4_request";

    /**
     * The ID of the access key.
     */
    private String secretKeyId;

    /**
     * The date from the authorization header.
     */
    private Instant date;

    /**
     * The region. E.g. "us-east-1"
     */
    private String region;

    /**
     * The service name. E.g. "s3"
     */
    private String service;

    /**
     * A list of signed headers in the request.
     */
    private List<String> signedHeaders;

    /**
     * The signature. This is a SHA-256 HMAC in hexadecimal format.
     */
    private String signature;

    /**
     * Matches an authorization header. A header looks like this:
     *
     * AWS4-HMAC-SHA256 Credential=AKIAI6FEC7AFNXEDNGUA/20170225/us-east-1/s3/aws4_request,
     *   SignedHeaders=host;x-amz-content-sha256;x-amz-date,
     *   Signature=27e44bb4f43d5e7f5575848a86a63eb45250577bdd9cc8127828b4243b8079f7
     */
    private static final Pattern HEADER_PATTERN = Pattern.compile(
            SIGV4SigningKeyCalculator.AWS4_HMAC_SHA256 + "\\s+Credential=" +
                    "(?<key>[^/]+)/" +
                    "(?<date>\\d{8})/" +
                    "(?<region>[^/]+)/" +
                    "(?<service>[^/]+)/" +
                    "aws4_request\\s*,\\s*SignedHeaders=" +
                    "(?<headers>[^,]+)\\s*,\\s*Signature=" +
                    "(?<signature>.*)");

    /**
     * Generate a SIGV4Authorization from an authorization header. This should be
     * compared with the authorization calculated from the request.
     */
    public static SIGV4Authorization fromAuthorizationHeader(String header) {
        final Matcher matcher = HEADER_PATTERN.matcher(header);
        if (matcher.matches()) {
            final String secretKeyId = matcher.group("key");
            final Instant date;
            try {
                date = SIGV4Utils.parseDate(matcher.group("date"));
            } catch (DateTimeParseException e) {
                final String error = String.format("Unable to parse credential date %s", matcher.group("date"));
                throw new AuthorizationHeaderMalformedException(error);
            }
            final String region = matcher.group("region");
            final String service = matcher.group("service");
            final List<String> headers = SIGV4Utils.parseHeaderList(matcher.group("headers"));
            final String signature = matcher.group("signature");
            return new SIGV4Authorization(
                    secretKeyId,
                    date,
                    region,
                    service,
                    headers,
                    signature);
        } else {
            final String error = String.format("Unable to parse Authorization header %s", header);
            throw new AuthorizationHeaderMalformedException(error);
        }
    }

    /**
     * Create a SIGV4Authorization from query parameters.
     */
    public static SIGV4Authorization fromQueryParameters(Map<String, String> parameters) {
        // A presigned URL uses query parameters like this:
        //
        // https://bucket.s3.amazonaws.com/file.txt?
        //   X-Amz-Algorithm=AWS4-HMAC-SHA256&
        //   X-Amz-Date=20170226T222950Z&
        //   X-Amz-SignedHeaders=host&
        //   X-Amz-Expires=899&
        //   X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20170226%2Fus-east-1%2Fs3%2Faws4_request&
        //   X-Amz-Signature=fa0f7be6df8ab8697f19a6e88f9702908c4c4f25275e12e3f9c5de34b53153f3
        final String algorithm = getParameter(X_AMZ_ALGORITHM, parameters);
        final String signedHeaders = getParameter(X_AMZ_SIGNED_HEADERS, parameters);
        final String credential = getParameter(X_AMZ_CREDENTIAL, parameters);
        final String signature = getParameter(X_AMZ_SIGNATURE, parameters);

        if (!SIGV4SigningKeyCalculator.AWS4_HMAC_SHA256.equals(algorithm)) {
            final String error = String.format("Invalid %s (%s). Expected %s", X_AMZ_ALGORITHM, algorithm,
                    SIGV4SigningKeyCalculator.AWS4_HMAC_SHA256);
            throw new AuthorizationQueryParametersException(error);
        }

        final List<String> headers = SIGV4Utils.parseHeaderList(signedHeaders);
        final String[] credentialComponents = credential.split("/");
        if (credentialComponents.length != 5) {
            final String error = String.format("Invalid X-Amz-Credential (%s)", credential);
            throw new AuthorizationQueryParametersException(error);
        }
        final String secretKeyId = credentialComponents[0];
        final Instant date;
        try {
            date = SIGV4Utils.parseDate(credentialComponents[1]);
        } catch (DateTimeParseException e) {
            final String error = String.format("Unable to parse credential date %s", credentialComponents[1]);
            throw new AuthorizationQueryParametersException(error);
        }
        final String region = credentialComponents[2];
        final String service = credentialComponents[3];
        if (!credentialComponents[4].equals(AWS4_REQUEST)) {
            final String error = String.format("Invalid %s (%s). Expected %s", X_AMZ_CREDENTIAL,
                    credentialComponents[4], AWS4_REQUEST);
            throw new AuthorizationQueryParametersException(error);
        }
        return new SIGV4Authorization(secretKeyId, date, region, service, headers, signature);
    }

    @VisibleForTesting
    SIGV4Authorization(String secretKeyId,
                       Instant date,
                       String region,
                       String service,
                       Iterable<String> signedHeaders,
                       String signature) {
        this.secretKeyId = Preconditions.checkNotNull(secretKeyId);
        this.date = Preconditions.checkNotNull(date).truncatedTo(ChronoUnit.DAYS);
        this.region = Preconditions.checkNotNull(region);
        this.service = Preconditions.checkNotNull(service);
        this.signedHeaders = ImmutableList.copyOf(Preconditions.checkNotNull(signedHeaders));
        this.signature = Preconditions.checkNotNull(signature);
    }

    public String getSecretKeyId() {
        return secretKeyId;
    }

    public Instant getDate() {
        return date;
    }

    public String getRegion() {
        return region;
    }

    public String getService() {
        return service;
    }

    public List<String> getSignedHeaders() {
        return signedHeaders;
    }

    public String getSignature() {
        return signature;
    }

    /**
     * This returns the Authorization header format.
     */
    public String toString() {
        return String.format("%s Credential=%s,SignedHeaders=%s,Signature=%s",
                AWS4_HMAC_SHA256,
                this.credential(),
                SIGV4Utils.headerList(this.signedHeaders),
                signature);

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SIGV4Authorization that = (SIGV4Authorization) o;
        return Objects.equals(secretKeyId, that.secretKeyId) &&
                Objects.equals(date, that.date) &&
                Objects.equals(region, that.region) &&
                Objects.equals(service, that.service) &&
                Objects.equals(signedHeaders, that.signedHeaders) &&
                Objects.equals(signature, that.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(secretKeyId, date, region, service, signedHeaders, signature);
    }

    /**
     * Gets the credential string of
     * [key-id]/[date]/[region]/[service]/aws4_request
     */
    private String credential() {
        return String.format("%s/%s/%s/%s/aws4_request", this.secretKeyId, SIGV4Utils.formatDate(this.date),
                this.region, this.service);
    }

    /**
     * Used to get a parameter from a query string collection. Throws an exception
     * if the parameter isn't present.
     */
    private static String getParameter(String parameter, Map<String, String> parameters) {
        String value = parameters.get(parameter);
        if (value == null) {
            throw new AuthorizationQueryParametersException(parameter + " not specified");
        }
        return value;
    }
}
