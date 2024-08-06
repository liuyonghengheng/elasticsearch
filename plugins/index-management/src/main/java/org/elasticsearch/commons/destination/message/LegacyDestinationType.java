

package org.elasticsearch.commons.destination.message;

/**
 * Supported legacy notification destinations for Alerting and Index Management
 */
public enum LegacyDestinationType {
    LEGACY_CHIME,
    LEGACY_SLACK,
    LEGACY_CUSTOM_WEBHOOK,
    LEGACY_EMAIL,
    LEGACY_SNS
}
