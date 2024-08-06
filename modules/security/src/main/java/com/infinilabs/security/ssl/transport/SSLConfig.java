/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.infinilabs.security.ssl.transport;

import com.infinilabs.security.support.ConfigConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

public class SSLConfig {

    public static final Setting<Boolean> SSL_DUAL_MODE_SETTING = Setting.boolSetting(ConfigConstants.SECURITY_CONFIG_SSL_DUAL_MODE_ENABLED,
            false, Setting.Property.NodeScope, Setting.Property.Dynamic); // Not filtered

    private static final Logger logger = LogManager.getLogger(SSLConfig.class);

    private final boolean sslOnly;
    private volatile boolean dualModeEnabled;

    public SSLConfig(final boolean sslOnly, final boolean dualModeEnabled) {
        this.sslOnly = sslOnly;
        this.dualModeEnabled = dualModeEnabled;
        if (this.dualModeEnabled && !this.sslOnly) {
            logger.warn("security_config.ssl_dual_mode_enabled is enabled but security.ssl_only mode is disabled. "
                + "SSL Dual mode is supported only when security plugin is in ssl_only mode");
        }
        logger.info("SSL dual mode is {}", isDualModeEnabled() ? "enabled" : "disabled");
    }

    public SSLConfig(final Settings settings) {
        this(settings.getAsBoolean(ConfigConstants.SECURITY_SSL_ONLY, false),
            settings.getAsBoolean(ConfigConstants.SECURITY_CONFIG_SSL_DUAL_MODE_ENABLED, false));
    }

    public void registerClusterSettingsChangeListener(final ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(SSL_DUAL_MODE_SETTING,
            dualModeEnabledClusterSetting -> {
                logger.info("Detected change in settings, cluster setting for SSL dual mode is {}", dualModeEnabledClusterSetting ? "enabled" : "disabled");
                setDualModeEnabled(dualModeEnabledClusterSetting);
            });
    }

    private void setDualModeEnabled(boolean dualModeEnabled) {
        this.dualModeEnabled = dualModeEnabled;
    }

    public boolean isDualModeEnabled() {
        // currently dual mode can be enabled only when SSLOnly is enabled. This stance can change in future.
        return sslOnly && dualModeEnabled;
    }

    public boolean isSslOnlyMode() {
        return sslOnly;
    }
}
