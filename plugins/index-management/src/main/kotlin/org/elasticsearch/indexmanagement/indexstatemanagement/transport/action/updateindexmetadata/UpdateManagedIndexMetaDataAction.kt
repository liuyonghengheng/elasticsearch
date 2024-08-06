/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.common.io.stream.Writeable

class UpdateManagedIndexMetaDataAction : ActionType<AcknowledgedResponse>(NAME, reader) {

    companion object {
        const val NAME = "cluster:admin/ilm/update/managedindexmetadata"
        val INSTANCE = UpdateManagedIndexMetaDataAction()

        val reader = Writeable.Reader { AcknowledgedResponse(it) }
    }

    override fun getResponseReader(): Writeable.Reader<AcknowledgedResponse> = reader
}
