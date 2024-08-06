

package org.elasticsearch.jobscheduler.spi;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public interface ScheduledJobParser {
    ScheduledJobParameter parse(XContentParser xContentParser, String id, JobDocVersion jobDocVersion) throws IOException;
}
