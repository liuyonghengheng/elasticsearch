/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.spi.indexstatemanagement

import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.monitor.jvm.JvmService
import java.util.Locale

abstract class Validate(
    val settings: Settings,
    val clusterService: ClusterService,
    val jvmService: JvmService
) {

    var validationStatus = ValidationStatus.PASSED
    var validationMessage: String? = "Starting Validation"

    abstract fun execute(indexName: String): Validate

    enum class ValidationStatus(val status: String) : Writeable {
        PASSED("passed"),
        RE_VALIDATING("re_validating"),
        FAILED("failed");

        override fun toString(): String {
            return status
        }

        override fun writeTo(out: StreamOutput) {
            out.writeString(status)
        }

        companion object {
            fun read(streamInput: StreamInput): ValidationStatus {
                return valueOf(streamInput.readString().uppercase(Locale.ROOT))
            }
        }
    }
}
