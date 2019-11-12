package com.oracle.pic.casper.webserver.api.auth;

import com.google.common.base.Preconditions;
import com.oracle.pic.identity.authentication.Principal;
import com.oracle.pic.identity.authentication.PrincipalImpl;
import com.oracle.pic.identity.authorization.sdk.AuthorizationRequest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

/**
 * A wrapper class to hold authentication information.
 *
 * There are three different kinds of authentication as follows:
 * <pre>
 * Authentication Type  User Principal  Service Principal  Description
 * USER                 Not Null        Null               Authenticated request signed by user's private key
 * INSTANCE             Not Null        Null               Authenticated request signed by instance's certificate
 * SERVICE              Null            Not Null           Authenticated request
 * OBO                  Not Null        Not Null
 * </pre>
 */
public final class AuthenticationInfo {

    // Represents request from super user that will bypass authz checks. Useful for Casper-internal callers.
    public static final AuthenticationInfo SUPER_USER = AuthenticationInfo.fromPrincipal(
            new PrincipalImpl("SuperUserTenant", "SuperUserSubject"));

    // Represents request from super user that will bypass authz checks. Useful for Sparta callers that rely on V1.
    public static final AuthenticationInfo V1_USER = AuthenticationInfo.fromPrincipal(
            new PrincipalImpl("V1UserTenant", "V1UserSubject"));

    // Represents request where no authentication information was passed over. In this case it is treated as a request
    // from anonymous user.
    public static final AuthenticationInfo ANONYMOUS_USER = AuthenticationInfo.fromPrincipal(
            new PrincipalImpl("AnonymousUserTenant", "AnonymousUserSubject"));

    @Nullable
    private final Principal userPrincipal;
    @Nullable
    private final Principal servicePrincipal;
    @Nonnull
    private final AuthorizationRequest.Kind requestKind;

    public static AuthenticationInfo fromPrincipal(Principal principal) {
        switch (principal.getType()) {
            case USER:
                return new AuthenticationInfo(principal, null, AuthorizationRequest.Kind.USER);
            case SERVICE:
                return new AuthenticationInfo(null, principal, AuthorizationRequest.Kind.SERVICE);
            case RESOURCE:
            case INSTANCE:
                return new AuthenticationInfo(principal, null, AuthorizationRequest.Kind.INSTANCE);
            default:
                throw new IllegalArgumentException("Unrecognized principal type: " + principal.getType());
        }
    }

    public AuthenticationInfo(
            Principal userPrincipal,
            Principal servicePrincipal,
            AuthorizationRequest.Kind requestKind
    ) {
        switch (requestKind) {
            case USER:
            case INSTANCE:
                Preconditions.checkArgument(servicePrincipal == null);
                Preconditions.checkNotNull(userPrincipal);
                break;
            case SERVICE:
                Preconditions.checkArgument(userPrincipal == null);
                Preconditions.checkNotNull(servicePrincipal);
                break;
            case OBO:
                Preconditions.checkNotNull(userPrincipal);
                Preconditions.checkNotNull(servicePrincipal);
                break;
            case DELEGATION:
                Preconditions.checkNotNull(userPrincipal);
                Preconditions.checkNotNull(servicePrincipal);
                break;
            default:
                throw new IllegalArgumentException("Unexpected request kind: " + requestKind);
        }
        this.userPrincipal = userPrincipal;
        this.servicePrincipal = servicePrincipal;
        this.requestKind = requestKind;
    }

    @Override
    public String toString() {
        return "AuthenticationInfo{" +
                "userPrincipal=" + userPrincipal +
                "servicePrincipal=" + servicePrincipal +
                ", requestKind=" + requestKind +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AuthenticationInfo that = (AuthenticationInfo) o;
        return Objects.equals(userPrincipal, that.userPrincipal) &&
                Objects.equals(servicePrincipal, that.servicePrincipal) &&
                requestKind == that.requestKind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(userPrincipal, servicePrincipal, requestKind);
    }

    public Optional<Principal> getUserPrincipal() {
        return Optional.ofNullable(userPrincipal);
    }

    public Optional<Principal> getServicePrincipal() {
        return Optional.ofNullable(servicePrincipal);
    }

    /**
     * Returns the main principal as follows:
     * Authentication Type          Principal
     * USER                         User
     * INSTANCE                     User
     * SERVICE                      Service
     * OBO                          User
     * @return the main principal
     */
    public Principal getMainPrincipal() {
        switch (requestKind) {
            case USER:
            case INSTANCE:
                return userPrincipal;
            case SERVICE:
                return servicePrincipal;
            case OBO:
            case DELEGATION:
                return userPrincipal;
            default:
                throw new IllegalArgumentException("Unexpected input: " + requestKind);
        }
    }

    public AuthorizationRequest.Kind getRequestKind() {
        return requestKind;
    }
}
