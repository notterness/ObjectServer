package com.oracle.pic.casper.webserver.auth.dataplane.model;

import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;
import com.oracle.pic.casper.webserver.auth.dataplane.exceptions.NotAuthenticatedException;
import org.apache.http.HttpHeaders;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * AuthUserAndPass encapsulates the username and password for an HTTP basic auth request.
 */
public final class AuthUserAndPass {

    private static final String BASIC_AUTH_PREFIX = "Basic ";

    private final String user;
    private final String pass;

    /**
     * Parse the username and password from HTTP headers.
     *
     * The username and password are parsed from the "Authorization" header, as described in RFC 7617.
     *
     * @param headers the headers of the request, which are expected to contain the Authorization header.
     * @return the AuthUserAndPass containing the parsed username and password.
     * @throws NotAuthenticatedException if the Authorization header is missing or malformed. The message in the
     *         exception is suitable for display to clients.
     */
    public static AuthUserAndPass fromHeaders(Map<String, List<String>> headers) {
        final List<String> authzHeaders = headers.get(HttpHeaders.AUTHORIZATION);
        if (authzHeaders == null) {
            throw new NotAuthenticatedException("The Authorization header must be present");
        } else if (authzHeaders.size() > 1) {
            throw new NotAuthenticatedException("There can be only one Authorization header in the request");
        }

        final String authzHeader = authzHeaders.get(0);
        if (!authzHeader.startsWith(BASIC_AUTH_PREFIX)) {
            throw new NotAuthenticatedException(
                    "The Authorization header must start with 'Basic', as specified in RFC 7617");
        }

        final String b64authzHeader = authzHeader.substring(BASIC_AUTH_PREFIX.length());
        final String userPass = new String(Base64.getDecoder().decode(b64authzHeader), Charsets.UTF_8);
        final int colonIdx = userPass.indexOf(':');
        if (colonIdx == -1) {
            throw new NotAuthenticatedException("The Authorization header must contain a colon separated " +
                    "username and password, as specified in RFC 7617");
        }

        final String username = userPass.substring(0, colonIdx);
        final String password = userPass.substring(colonIdx + 1, userPass.length());
        return new AuthUserAndPass(username, password);
    }

    public AuthUserAndPass(String user, String pass) {
        this.user = user;
        this.pass = pass;
    }

    public String getUsername() {
        return user;
    }

    public String getPassword() {
        return pass;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AuthUserAndPass that = (AuthUserAndPass) o;
        return Objects.equals(user, that.user) &&
                Objects.equals(pass, that.pass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, pass);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("user", user)
                .add("pass", pass)
                .toString();
    }
}
