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

package org.elasticsearch.replication.util

import org.elasticsearch.replication.action.autofollow.UpdateAutoFollowPatternAction
import org.elasticsearch.replication.action.changes.GetChangesAction
import org.elasticsearch.replication.action.index.ReplicateIndexAction
import org.elasticsearch.replication.action.pause.PauseIndexReplicationAction
import org.elasticsearch.replication.action.replay.ReplayChangesAction
import org.elasticsearch.replication.action.repository.GetFileChunkAction
import org.elasticsearch.replication.action.resume.ResumeIndexReplicationAction
import org.elasticsearch.replication.action.status.ReplicationStatusAction
import org.elasticsearch.replication.action.stop.StopIndexReplicationAction
import org.elasticsearch.replication.action.update.UpdateIndexReplicationAction
import org.elasticsearch.replication.metadata.store.ReplicationMetadata
import org.elasticsearch.commons.ConfigConstants
import org.elasticsearch.commons.authuser.User
import org.apache.logging.log4j.LogManager
import org.elasticsearch.common.util.concurrent.ThreadContext

class SecurityContext {
    companion object {
        private val log = LogManager.getLogger(SecurityContext::class.java)
        const val _SECURITY_USER = "_security_user"
        const val SECURITY_INJECTED_ROLES_VALIDATION = "opendistro_security_injected_roles_validation"
        const val REPLICATION_PLUGIN_USER = "ccr_user"

        val ADMIN_USER = User(REPLICATION_PLUGIN_USER, null, listOf("all_access"), null)

        val ALL_TRANSIENTS = listOf(ConfigConstants.SECURITY_INJECTED_ROLES,
                ConfigConstants.INJECTED_USER, _SECURITY_USER)

        val LEADER_USER_ACTIONS = listOf(GetChangesAction.NAME, GetFileChunkAction.NAME)
        val FOLLOWER_USER_ACTIONS = listOf(ReplayChangesAction.NAME,
                ReplicateIndexAction.NAME, PauseIndexReplicationAction.NAME,
                ResumeIndexReplicationAction.NAME, StopIndexReplicationAction.NAME,
                UpdateIndexReplicationAction.NAME, ReplicationStatusAction.NAME,
                UpdateAutoFollowPatternAction.NAME)

        fun fromSecurityThreadContext(threadContext: ThreadContext): User? {
            var userInfo = threadContext.getTransient<String?>(ConfigConstants._SECURITY_USER_INFO_THREAD_CONTEXT)
            val userObj = threadContext.getTransient<Any?>(_SECURITY_USER)
            if(userInfo == null && userObj != null) {
                // Case: When admin certs are used, security plugin skips populating the user info in thread context.
                // If userObj(obj) is present and userInfo(String) is not populated, assuming admin role for the user and
                // only passed role(use_roles) in the request is stored after checks (as admin should have access to all roles)
                userInfo = "adminDN|"
            }
            return User.parse(userInfo)
        }

        fun asUserInjection(threadContext: ThreadContext, userString: String?) {
            if(userString != null) {
                val userInfo = threadContext.getTransient<String?>(ConfigConstants.INJECTED_USER)
                if (userInfo != null) {
                    log.warn("Injected user not empty in thread context $userInfo")
                }
                else {
                    threadContext.putTransient(ConfigConstants.INJECTED_USER, userString)
                }
            }
        }

        fun asRolesInjection(threadContext: ThreadContext, role: String?) {
            if(role != null) {
                val rolesInj = threadContext.getTransient<String?>(ConfigConstants.SECURITY_INJECTED_ROLES)
                if(rolesInj != null) {
                    log.warn("Injected roles not empty in thread context $rolesInj")
                }
                else {
                    threadContext.putTransient(ConfigConstants.SECURITY_INJECTED_ROLES, role)
                }
            }
        }

        fun setBasedOnActions(replMetadata: ReplicationMetadata?, action: String, threadContext: ThreadContext) {
            if(replMetadata != null) {
                if(LEADER_USER_ACTIONS.contains(action)) {
                    asRolesInjection(threadContext, replMetadata.leaderContext.user?.toInjectedRoles())
                    return
                } else if(FOLLOWER_USER_ACTIONS.contains(action)) {
                    asRolesInjection(threadContext, replMetadata.followerContext.user?.toInjectedRoles())
                    return
                }
            }
            // For all other requests - using admin
            asRolesInjection(threadContext, ADMIN_USER.toInjectedRoles())
        }
    }
}
