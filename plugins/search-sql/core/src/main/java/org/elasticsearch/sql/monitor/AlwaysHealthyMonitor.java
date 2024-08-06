

package org.elasticsearch.sql.monitor;

/**
 * Always healthy resource monitor.
 */
public class AlwaysHealthyMonitor extends ResourceMonitor {
  public static final ResourceMonitor ALWAYS_HEALTHY_MONITOR =
      new AlwaysHealthyMonitor();

  /**
   * always healthy.
   */
  @Override
  public boolean isHealthy() {
    return true;
  }
}
