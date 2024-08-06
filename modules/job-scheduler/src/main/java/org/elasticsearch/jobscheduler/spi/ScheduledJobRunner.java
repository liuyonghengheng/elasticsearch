

package org.elasticsearch.jobscheduler.spi;

public interface ScheduledJobRunner {
    void runJob(ScheduledJobParameter job, JobExecutionContext context);
}
