


package org.elasticsearch.sql.search.monitor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.sql.common.setting.Settings;

@ExtendWith(MockitoExtension.class)
class ElasticsearchResourceMonitorTest {

  @Mock
  private Settings settings;

  @Mock
  private ElasticsearchMemoryHealthy memoryMonitor;

  @BeforeEach
  public void setup() {
    when(settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT))
        .thenReturn(new ByteSizeValue(10L));
  }

  @Test
  void isHealthy() {
    when(memoryMonitor.isMemoryHealthy(anyLong())).thenReturn(true);

    ElasticsearchResourceMonitor resourceMonitor =
        new ElasticsearchResourceMonitor(settings, memoryMonitor);
    assertTrue(resourceMonitor.isHealthy());
  }

  @Test
  void notHealthyFastFailure() {
    when(memoryMonitor.isMemoryHealthy(anyLong())).thenThrow(
        ElasticsearchMemoryHealthy.MemoryUsageExceedFastFailureException.class);

    ElasticsearchResourceMonitor resourceMonitor =
        new ElasticsearchResourceMonitor(settings, memoryMonitor);
    assertFalse(resourceMonitor.isHealthy());
    verify(memoryMonitor, times(1)).isMemoryHealthy(anyLong());
  }

  @Test
  void notHealthyWithRetry() {
    when(memoryMonitor.isMemoryHealthy(anyLong())).thenThrow(
        ElasticsearchMemoryHealthy.MemoryUsageExceedException.class);

    ElasticsearchResourceMonitor resourceMonitor =
        new ElasticsearchResourceMonitor(settings, memoryMonitor);
    assertFalse(resourceMonitor.isHealthy());
    verify(memoryMonitor, times(3)).isMemoryHealthy(anyLong());
  }

  @Test
  void healthyWithRetry() {

    when(memoryMonitor.isMemoryHealthy(anyLong())).thenThrow(
        ElasticsearchMemoryHealthy.MemoryUsageExceedException.class).thenReturn(true);

    ElasticsearchResourceMonitor resourceMonitor =
        new ElasticsearchResourceMonitor(settings, memoryMonitor);
    assertTrue(resourceMonitor.isHealthy());
    verify(memoryMonitor, times(2)).isMemoryHealthy(anyLong());
  }
}
