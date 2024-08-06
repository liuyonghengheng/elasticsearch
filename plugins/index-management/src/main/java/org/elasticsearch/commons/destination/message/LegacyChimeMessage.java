

package org.elasticsearch.commons.destination.message;

import java.io.IOException;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;

/**
 * This class holds the contents of an Chime message
 */
public class LegacyChimeMessage extends LegacyBaseMessage {
    private final String message;

    private LegacyChimeMessage(final String destinationName, final String url, final String message) {
        super(LegacyDestinationType.LEGACY_CHIME, destinationName, message, url);

        if (Strings.isNullOrEmpty(message)) {
            throw new IllegalArgumentException("Message content is missing");
        }

        this.message = message;
    }

    public LegacyChimeMessage(StreamInput streamInput) throws IOException {
        super(streamInput);
        this.message = super.getMessageContent();
    }

    @Override
    public String toString() {
        return "DestinationType: " + getChannelType() + ", DestinationName:" + destinationName + ", Url: " + url + ", Message: <...>";
    }

    public static class Builder {
        private String message;
        private final String destinationName;
        private String url;

        public Builder(String destinationName) {
            this.destinationName = destinationName;
        }

        public Builder withMessage(String message) {
            this.message = message;
            return this;
        }

        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public LegacyChimeMessage build() {
            return new LegacyChimeMessage(this.destinationName, this.url, this.message);
        }
    }

    public String getMessage() {
        return message;
    }

    public String getUrl() {
        return url;
    }
}
