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

package org.elasticsearch.replication.metadata.store

data class DeleteReplicationMetadataRequest(val metadataType: String,
                                       val connectionName: String?,
                                       val resourceName: String) {
}
