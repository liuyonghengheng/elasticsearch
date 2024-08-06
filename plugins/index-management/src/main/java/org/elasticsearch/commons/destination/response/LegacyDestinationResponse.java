

package org.elasticsearch.commons.destination.response;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

/**
 * This class is a place holder for destination response metadata
 */
public class LegacyDestinationResponse extends LegacyBaseResponse {

    private final String responseContent;

    private LegacyDestinationResponse(final String responseString, final Integer statusCode) {
        super(statusCode);
        if (responseString == null) {
            throw new IllegalArgumentException("Response is missing");
        }
        this.responseContent = responseString;
    }

    public LegacyDestinationResponse(StreamInput streamInput) throws IOException {
        super(streamInput);
        this.responseContent = streamInput.readString();
    }

    public static class Builder {
        private String responseContent;
        private Integer statusCode;

        public Builder withResponseContent(String responseContent) {
            this.responseContent = responseContent;
            return this;
        }

        public Builder withStatusCode(Integer statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        public LegacyDestinationResponse build() {
            return new LegacyDestinationResponse(responseContent, statusCode);
        }
    }

    public String getResponseContent() {
        return this.responseContent;
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        super.writeTo(streamOutput);
        streamOutput.writeString(responseContent);
    }
}
