package com.oracle.pic.casper.webserver.api.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.oracle.pic.casper.webserver.api.v2.HttpPathQueryHelpers;
import com.oracle.pic.casper.webserver.util.WSRequestContext;
import com.oracle.pic.identity.authentication.error.AuthClientException;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Signing spec for PARs, which includes generating string to sign (STS) and nonce. We only sign the path of the request
 * but not the method because we don't want to confuse AuthN with AuthZ. Performing a PUT operation with a GET PAR
 * (assuming ownership of both) will authenticate in this case, because both PAR exists in the backend PAR store.
 * However, AuthZ will fail later in the PAR management handler.
 *
 * The signing helper will be used at 2 places.
 *
 *      1. PAR creation time when we have to compute the STS. Here the resource path is  /n/ /b/ /o/
 *      2. PAR use time when we have to auth the user - here the path is  /p/  /n/  /b/  /o/
 */
public final class ParSigningHelper {

    private static final Logger LOG = LoggerFactory.getLogger(ParSigningHelper.class);

    private static final String NAMESPACE = "namespace";
    private static final String BUCKET_NAME = "bucketName";
    private static final String OBJECT_NAME = "objectName";
    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final String CHAR_NAME = "UTF-8";
    private static final int NONCE_BYTES = 32;
    private static final SecureRandom SECURE_RANDOM_GENERATOR;

    /**
     *
     * Initializes the secure random generator
     */
    static {
        try {
            // 1. This instance is not seeded yet. The first call to nextBytes() will securely seed itself.
            // 2. We should always explicitly specify which generator we want. Some more security practices can be found
            // at https://www.cigital.com/justice-league-blog/2009/08/14/proper-use-of-javas-securerandom/
            SECURE_RANDOM_GENERATOR = SecureRandom.getInstance("SHA1PRNG", "SUN");
        } catch (NoSuchAlgorithmException ex) {
            LOG.error("Failed to create a secure random number generator. {}", ex);
            throw new RuntimeException("Failed to create a secure random number generator");
        } catch (NoSuchProviderException ex) {
            LOG.error("Failed to create a secure random number generator. {}", ex);
            throw new RuntimeException("Failed to create a secure random number generator");
        }
    }
    private ParSigningHelper() {

    }

    @VisibleForTesting
    static Map<String, String> getSTSForObjectLevelPars(HttpServerRequest request, RoutingContext context) {
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);
        String objectName = HttpPathQueryHelpers.getObjectName(request, wsRequestContext);

        return getSTS(namespace, bucketName, objectName);
    }

    static Map<String, String> getSTSForBucketLevelPars(HttpServerRequest request, RoutingContext context) {
        final WSRequestContext wsRequestContext = WSRequestContext.get(context);

        String namespace = HttpPathQueryHelpers.getNamespaceName(request, wsRequestContext);
        String bucketName = HttpPathQueryHelpers.getBucketName(request, wsRequestContext);

        return getSTS(namespace, bucketName, null);
    }

    public static String getVerifierId(Map<String, String> stringToSign, String nonce) {
        Preconditions.checkArgument(!StringUtils.isBlank(nonce), "nonce must not be null or empty");
        Preconditions.checkArgument(stringToSign != null, "stringToSign mush not be null");

        try {
            Mac hmacSHA256 = Mac.getInstance(HMAC_ALGORITHM);
            byte[] bytes = Base64.getUrlDecoder().decode(nonce);
            SecretKeySpec secretKeySpec = new SecretKeySpec(bytes, HMAC_ALGORITHM);
            hmacSHA256.init(secretKeySpec);

            Map<String, String> orderedStringToSign = new TreeMap<>(String::compareToIgnoreCase);

            for (Map.Entry<String, String> entry: stringToSign.entrySet()) {
                orderedStringToSign.put(entry.getKey().toLowerCase(), entry.getValue().toLowerCase());
            }

            for (String value: orderedStringToSign.values()) {
                hmacSHA256.update(value.getBytes(CHAR_NAME));
            }

            return Base64.getEncoder().encodeToString(hmacSHA256.doFinal());

        } catch (InvalidKeyException | NoSuchAlgorithmException | UnsupportedEncodingException |
                IllegalArgumentException e) {

            LOG.debug("Fail to generate verifier", e);
            throw new AuthClientException(e);
        }
    }

    public static String getNonce() {
        byte[] bytes = new byte[NONCE_BYTES];
        SECURE_RANDOM_GENERATOR.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    public static Map<String, String> getSTS(String namespace, String bucketName, @Nullable String objectName) {
        Map<String, String> stringToSign = new HashMap<>();
        stringToSign.put(NAMESPACE, namespace);
        stringToSign.put(BUCKET_NAME, bucketName);
        if (objectName != null) {
            stringToSign.put(OBJECT_NAME, objectName);
        }
        return stringToSign;
    }
}
