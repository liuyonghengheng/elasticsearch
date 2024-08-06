/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.elasticsearch.replication.action.setup

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.support.master.AcknowledgedResponse

class ValidatePermissionsAction private constructor(): ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse){
    companion object {
        const val NAME = "indices:admin/plugins/replication/index/setup/validate"
        val INSTANCE: ValidatePermissionsAction = ValidatePermissionsAction()
    }
}
