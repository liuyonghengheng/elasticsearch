/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.validation

import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.indexmanagement.util.OpenForTesting
import org.elasticsearch.indexmanagement.spi.indexstatemanagement.Validate
import org.elasticsearch.monitor.jvm.JvmService

@OpenForTesting
class ValidateNothing(
    settings: Settings,
    clusterService: ClusterService,
    jvmService: JvmService
) : Validate(settings, clusterService, jvmService) {

    // skips validation
    override fun execute(indexName: String): Validate {
        return this
    }
}
