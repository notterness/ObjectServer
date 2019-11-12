package com.oracle.pic.casper.webserver.api.ratelimit;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.oracle.pic.casper.mds.operator.GrpcEmbargoV3Rule;

import java.util.regex.Pattern;

/**
 * Represents an embargo rule. The rules are matches against an operation
 * which is represented by an {@link EmbargoV3Operation}.
 */
public final class EmbargoV3Rule {

    /**
     * ID of the rule.
     */
    private final long id;

    /**
     * Regular expression that matches the API (e.g. "s3"). Matches against
     * {@link EmbargoV3Operation#getApi()}.
     */
    private final Pattern apiRegex;

    /**
     * Regular expression that matches the operation (e.g. "getNamespace").
     * Matches against {@link EmbargoV3Operation#getObject()}.
     */
    private final Pattern operationRegex;

    /**
     * Regular expression that matches the namespace (e.g. "bmcostests").
     * Matches against {@link EmbargoV3Operation#getNamespace()}.
     */
    private final Pattern namespaceRegex;

    /**
     * Regular expression that matches the bucket (e.g. "backups.*"). Matches
     * against {@link EmbargoV3Operation#getBucket()}.
     */
    private final Pattern bucketRegex;

    /**
     * Regular expression that matches the object name (e.g. ".*\.tar\.gz").
     * Matches against {@link EmbargoV3Operation#getObject()}.
     */
    private final Pattern objectRegex;

    /**
     * The number of operations-per-second allowed for requests that match
     * this rule. If this is 0 then matching operations are not allowed
     * at all.
     */
    private final double allowedRate;

    private EmbargoV3Rule(
            long id,
            String apiRegex,
            String operationRegex,
            String namespaceRegex,
            String bucketRegex,
            String objectRegex,
            double allowedRate) {
        Preconditions.checkArgument(id >= 0);
        Preconditions.checkArgument(allowedRate >= 0.0);
        // At least one regex must be non-null
        Preconditions.checkArgument(
            !Strings.isNullOrEmpty(apiRegex) ||
            !Strings.isNullOrEmpty(operationRegex) ||
            !Strings.isNullOrEmpty(namespaceRegex) ||
            !Strings.isNullOrEmpty(bucketRegex) ||
            !Strings.isNullOrEmpty(objectRegex));
        this.id = id;
        this.apiRegex = compileRegex(apiRegex, CaseSetting.INSENSITIVE);
        this.operationRegex = compileRegex(operationRegex, CaseSetting.INSENSITIVE);
        this.namespaceRegex = compileRegex(namespaceRegex, CaseSetting.INSENSITIVE);
        this.bucketRegex = compileRegex(bucketRegex, CaseSetting.SENSITIVE);
        this.objectRegex = compileRegex(objectRegex, CaseSetting.SENSITIVE);
        this.allowedRate = allowedRate;
    }

    /**
     * Get the ID of the rule.
     */
    public long getId() {
        return id;
    }

    /**
     * Get the number of operations-per-second allowed for operations that match
     * this rule.
     */
    public double getAllowedRate() {
        return allowedRate;
    }

    /**
     * Returns true if the operation matches the rule.
     */
    public boolean matches(EmbargoV3Operation request) {
        if (request.getApi().equals(EmbargoV3Operation.Api.V1) && this.apiRegex == null) {
            // Special-case: If there is no API regex then we will NOT match
            // calls made to the V1 API.
            //
            // It is possible for the same namespace/bucket to exist in both the
            // V1 universe and the V2/Swift/S3 universe. We will not apply rule
            // blocking access to a specific namespace+bucket to both the V1
            // and V2 bucket (that is almost certainly NOT what the user intended).
            //
            // If there is no API specified we will assume the user wants to
            // apply the embargo rule to the new Swift/S3/V2 API and not the
            // deprecated V1 API.
            return false;
        }
        return matches(this.apiRegex, request.getApi().getName()) &&
            matches(this.operationRegex, request.getOperation().getSummary()) &&
            matches(this.namespaceRegex, request.getNamespace()) &&
            matches(this.bucketRegex, request.getBucket()) &&
            matches(this.objectRegex, request.getObject());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("apiRegex", apiRegex)
            .add("operationRegex", operationRegex)
            .add("namespaceRegex", namespaceRegex)
            .add("bucketRegex", bucketRegex)
            .add("objectRegex", objectRegex)
            .add("allowedRate", allowedRate)
            .toString();
    }

    /**
     * Convert a {@link GrpcEmbargoV3Rule} (returned by Operator MDS) to a
     * {@link EmbargoV3Rule} (used by the web-server).
     */
    public static EmbargoV3Rule fromGrpcEmbargoV3Rule(GrpcEmbargoV3Rule grpcRule) {
        return builder()
            .setId(grpcRule.getId())
            .setApiRegex(grpcRule.getApiRegex())
            .setOperationRegex(grpcRule.getOperationRegex())
            .setAllowedRate(grpcRule.getAllowedRate())
            .setNamespaceRegex(grpcRule.getNamespaceRegex())
            .setBucketRegex(grpcRule.getBucketRegex())
            .setObjectRegex(grpcRule.getObjectRegex())
            .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    String getApiRegex() {
        return apiRegex.pattern();
    }

    String getOperationRegex() {
        return operationRegex.pattern();
    }

    String getNamespaceRegex() {
        return namespaceRegex.pattern();
    }

    String getBucketRegex() {
        return bucketRegex.pattern();
    }

    String getObjectRegex() {
        return objectRegex.pattern();
    }

    private static boolean matches(Pattern p, String s) {
        if (p == null) {
            // Null patterns match everything.
            return true;
        } else if (s == null) {
            // On the other hand, a null string does not match any patterns.
            // For example, if the rule has an object-name regex and the
            // operation does not have an object-name at all (e.g. bucket
            // creation) then the rule will not match.
            return false;
        } else {
            // Match the pattern against the string. We use matches() because
            // we want to match the entire string, not a substring.
            return p.matcher(s).matches();
        }
    }

    private static Pattern compileRegex(String regex, CaseSetting caseSetting) {
        if (Strings.isNullOrEmpty(regex)) {
            return null;
        }
        // Use DOTALL in case we ever allow \r or \n in object names and
        // need to match against them.
        int flags = Pattern.DOTALL;
        if (caseSetting == CaseSetting.INSENSITIVE) {
            flags |= Pattern.CASE_INSENSITIVE;
        }
        final Pattern p = Pattern.compile(regex, flags);
        return p;
    }

    /**
     * Passed to {@link #compileRegex(String, CaseSetting)} to determine if we
     * want case-insensitive matching.
     */
    private enum CaseSetting {
        SENSITIVE,
        INSENSITIVE
    }

    public static final class Builder {
        private long id = -1;
        private String apiRegex;
        private String operationRegex;
        private String namespaceRegex;
        private String bucketRegex;
        private String objectRegex;
        private double allowedRate = -1.0;

        private Builder() {
        }

        public Builder setId(long id) {
            this.id = id;
            return this;
        }

        public Builder setApiRegex(String apiRegex) {
            this.apiRegex = apiRegex;
            return this;
        }

        public Builder setOperationRegex(String operationRegex) {
            this.operationRegex = operationRegex;
            return this;
        }

        public Builder setNamespaceRegex(String namespaceRegex) {
            this.namespaceRegex = namespaceRegex;
            return this;
        }

        public Builder setBucketRegex(String bucketRegex) {
            this.bucketRegex = bucketRegex;
            return this;
        }

        public Builder setObjectRegex(String objectRegex) {
            this.objectRegex = objectRegex;
            return this;
        }

        public Builder setAllowedRate(double allowedRate) {
            this.allowedRate = allowedRate;
            return this;
        }

        public EmbargoV3Rule build() {
            return new EmbargoV3Rule(
                id,
                apiRegex,
                operationRegex,
                namespaceRegex,
                bucketRegex,
                objectRegex,
                allowedRate);
        }
    }
}
