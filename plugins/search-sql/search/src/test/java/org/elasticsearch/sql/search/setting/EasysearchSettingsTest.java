


package org.elasticsearch.sql.search.setting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.sql.search.setting.LegacyOpenDistroSettings.legacySettings;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.sql.common.setting.LegacySettings;
import org.elasticsearch.sql.common.setting.Settings;

@ExtendWith(MockitoExtension.class)
class ElasticsearchSettingsTest {

  @Mock
  private ClusterSettings clusterSettings;

  @Test
  void getSettingValue() {
    SearchSettings settings = new SearchSettings(clusterSettings);
    ByteSizeValue sizeValue = settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT);

    assertNotNull(sizeValue);
  }

  @Test
  void pluginSettings() {
    List<Setting<?>> settings = SearchSettings.pluginSettings();

    assertFalse(settings.isEmpty());
  }

  @Test
  void getSettings() {
    SearchSettings settings = new SearchSettings(clusterSettings);
    assertFalse(settings.getSettings().isEmpty());
  }

  @Test
  void update() {
    SearchSettings settings = new SearchSettings(clusterSettings);
    ByteSizeValue oldValue = settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT);
    SearchSettings.Updater updater =
        settings.new Updater(Settings.Key.QUERY_MEMORY_LIMIT);
    updater.accept(new ByteSizeValue(0L));

    ByteSizeValue newValue = settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT);

    assertNotEquals(newValue.getBytes(), oldValue.getBytes());
  }

  @Test
  void settingsFallback() {
    SearchSettings settings = new SearchSettings(clusterSettings);
    assertEquals(
        settings.getSettingValue(Settings.Key.SQL_ENABLED),
        LegacyOpenDistroSettings.SQL_ENABLED_SETTING.get(
            org.elasticsearch.common.settings.Settings.EMPTY));
    assertEquals(
        settings.getSettingValue(Settings.Key.SQL_SLOWLOG),
        LegacyOpenDistroSettings.SQL_QUERY_SLOWLOG_SETTING.get(
            org.elasticsearch.common.settings.Settings.EMPTY));
    assertEquals(
        settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE),
        LegacyOpenDistroSettings.SQL_CURSOR_KEEPALIVE_SETTING.get(
            org.elasticsearch.common.settings.Settings.EMPTY));
    assertEquals(
        settings.getSettingValue(Settings.Key.PPL_ENABLED),
        LegacyOpenDistroSettings.PPL_ENABLED_SETTING.get(
            org.elasticsearch.common.settings.Settings.EMPTY));
    assertEquals(
        settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT),
        LegacyOpenDistroSettings.PPL_QUERY_MEMORY_LIMIT_SETTING.get(
            org.elasticsearch.common.settings.Settings.EMPTY));
    assertEquals(
        settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT),
        LegacyOpenDistroSettings.QUERY_SIZE_LIMIT_SETTING.get(
            org.elasticsearch.common.settings.Settings.EMPTY));
    assertEquals(
        settings.getSettingValue(Settings.Key.METRICS_ROLLING_WINDOW),
        LegacyOpenDistroSettings.METRICS_ROLLING_WINDOW_SETTING.get(
            org.elasticsearch.common.settings.Settings.EMPTY));
    assertEquals(
        settings.getSettingValue(Settings.Key.METRICS_ROLLING_INTERVAL),
        LegacyOpenDistroSettings.METRICS_ROLLING_INTERVAL_SETTING.get(
            org.elasticsearch.common.settings.Settings.EMPTY));
  }

  @Test
  public void updateLegacySettingsFallback() {
    org.elasticsearch.common.settings.Settings settings =
        org.elasticsearch.common.settings.Settings.builder()
            .put(LegacySettings.Key.SQL_ENABLED.getKeyValue(), false)
            .put(LegacySettings.Key.SQL_QUERY_SLOWLOG.getKeyValue(), 10)
            .put(LegacySettings.Key.SQL_CURSOR_KEEPALIVE.getKeyValue(), timeValueMinutes(1))
            .put(LegacySettings.Key.PPL_ENABLED.getKeyValue(), true)
            .put(LegacySettings.Key.PPL_QUERY_MEMORY_LIMIT.getKeyValue(), "20%")
            .put(LegacySettings.Key.QUERY_SIZE_LIMIT.getKeyValue(), 100)
            .put(LegacySettings.Key.METRICS_ROLLING_WINDOW.getKeyValue(), 2000L)
            .put(LegacySettings.Key.METRICS_ROLLING_INTERVAL.getKeyValue(), 100L)
            .build();

    assertEquals(SearchSettings.SQL_ENABLED_SETTING.get(settings), false);
    assertEquals(SearchSettings.SQL_SLOWLOG_SETTING.get(settings), 10);
    assertEquals(SearchSettings.SQL_CURSOR_KEEP_ALIVE_SETTING.get(settings),
        timeValueMinutes(1));
    assertEquals(SearchSettings.PPL_ENABLED_SETTING.get(settings), true);
    assertEquals(SearchSettings.QUERY_MEMORY_LIMIT_SETTING.get(settings),
        new ByteSizeValue((int) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.2)));
    assertEquals(SearchSettings.QUERY_SIZE_LIMIT_SETTING.get(settings), 100);
    assertEquals(SearchSettings.METRICS_ROLLING_WINDOW_SETTING.get(settings), 2000L);
    assertEquals(SearchSettings.METRICS_ROLLING_INTERVAL_SETTING.get(settings), 100L);
  }


  @Test
  void legacySettingsShouldBeDeprecatedBeforeRemove() {
    assertEquals(15, legacySettings().size());
  }
}
