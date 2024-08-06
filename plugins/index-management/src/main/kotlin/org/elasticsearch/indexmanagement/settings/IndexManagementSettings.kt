/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.elasticsearch.indexmanagement.settings

import org.elasticsearch.common.settings.Setting

@Suppress("UtilityClassWithPublicConstructor")
class IndexManagementSettings {

    companion object {

        val FILTER_BY_BACKEND_ROLES: Setting<Boolean> = Setting.boolSetting(
            "plugins.index_management.filter_by_backend_roles",
            false,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    }
}
