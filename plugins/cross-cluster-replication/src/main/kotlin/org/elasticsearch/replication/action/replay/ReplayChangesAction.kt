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

package org.elasticsearch.replication.action.replay

import org.elasticsearch.action.ActionType

class ReplayChangesAction private constructor() : ActionType<ReplayChangesResponse>(NAME, ::ReplayChangesResponse) {

    companion object {
        const val NAME = "indices:data/write/plugins/replication/changes"
        val INSTANCE = ReplayChangesAction()
    }
}
