


package org.elasticsearch.jdbc.auth;

/**
 * Enum representing supported authentication methods
 *
 */
public enum AuthenticationType {

    /**
     * No authentication
     */
    NONE,

    /**
     * HTTP Basic authentication
     */
    BASIC,

    /**
     * AWS Signature V4
     */
    AWS_SIGV4;
}
