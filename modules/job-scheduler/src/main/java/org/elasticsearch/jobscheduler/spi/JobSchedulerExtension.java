

package org.elasticsearch.jobscheduler.spi;

/**
 * SPI of job scheduler.
 */
public interface JobSchedulerExtension {
    /**
     * @return job type string.
     */
    String getJobType();

    /**
     * @return job index name.
     */
    String getJobIndex();

    /**
     * @return job runner implementation.
     */
    ScheduledJobRunner getJobRunner();

    /**
     * @return job document parser.
     */
    ScheduledJobParser getJobParser();
}
