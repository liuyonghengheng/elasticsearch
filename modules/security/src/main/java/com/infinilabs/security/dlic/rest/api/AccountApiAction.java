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
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.infinilabs.security.dlic.rest.api;

import com.infinilabs.security.auditlog.AuditLog;
import com.infinilabs.security.configuration.AdminDNs;
import com.infinilabs.security.configuration.ConfigurationRepository;
import com.infinilabs.security.dlic.rest.validation.AbstractConfigurationValidator;
import com.infinilabs.security.dlic.rest.validation.AccountValidator;
import com.infinilabs.security.privileges.PrivilegesEvaluator;
import com.infinilabs.security.securityconf.Hashed;
import com.infinilabs.security.securityconf.impl.CType;
import com.infinilabs.security.securityconf.impl.SecurityDynamicConfiguration;
import com.infinilabs.security.ssl.transport.PrincipalExtractor;
import com.infinilabs.security.support.ConfigConstants;
import com.infinilabs.security.support.SecurityJsonNode;
import com.infinilabs.security.user.User;
import com.fasterxml.jackson.databind.JsonNode;
import com.infinilabs.security.dlic.rest.support.Utils;
import com.infinilabs.security.util.OpenBSDBCrypt;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;

/**
 * Rest API action to fetch or update account details of the signed-in user.
 * Currently this action serves GET and PUT request for /_security/account endpoint
 */
public class AccountApiAction extends AbstractApiAction {
    private static final String RESOURCE_NAME = "account";
    private static final List<Route> routes = ImmutableList.of(
            new Route(Method.GET, "/_security/account"),
            new Route(Method.PUT, "/_security/account")
    );

    private final PrivilegesEvaluator privilegesEvaluator;
    private final ThreadContext threadContext;

    public AccountApiAction(Settings settings,
                            Path configPath,
                            RestController controller,
                            Client client,
                            AdminDNs adminDNs,
                            ConfigurationRepository cl,
                            ClusterService cs,
                            PrincipalExtractor principalExtractor,
                            PrivilegesEvaluator privilegesEvaluator,
                            ThreadPool threadPool,
                            AuditLog auditLog) {
        super(settings, configPath, controller, client, adminDNs, cl, cs, principalExtractor, privilegesEvaluator, threadPool, auditLog);
        this.privilegesEvaluator = privilegesEvaluator;
        this.threadContext = threadPool.getThreadContext();
    }

    @Override
    public List<Route> routes() {
        return routes;
    }

    /**
     * GET request to fetch account details
     *
     * Sample request:
     * GET _security/account
     *
     * Sample response:
     * {
     *   "username" : "test",
     *   "reserved" : false,
     *   "hidden" : false,
     *   "builtin" : true,
     *   "external_roles" : [ ],
     *   "attributes" : [ ],
     *   "roles" : [
     *     "own_index"
     *   ]
     * }
     *
     * @param channel channel to return response
     * @param request request to be served
     * @param client client
     * @param content content body
     * @throws IOException
     */
    @Override
    protected void handleGet(RestChannel channel, RestRequest request, Client client, final JsonNode content) throws IOException {
        final XContentBuilder builder = channel.newBuilder();
        BytesRestResponse response;

        try {
            builder.startObject();
            final User user = threadContext.getTransient(ConfigConstants.SECURITY_USER);
            if (user != null) {
                final TransportAddress remoteAddress = threadContext.getTransient(ConfigConstants.SECURITY_REMOTE_ADDRESS);
                final Set<String> securityRoles = privilegesEvaluator.mapRoles(user, remoteAddress);
                final SecurityDynamicConfiguration<?> configuration = load(getConfigName(), false);

                builder.field("username", user.getName())
                        .field("reserved", isReserved(configuration, user.getName()))
                        .field("hidden", configuration.isHidden(user.getName()))
                        .field("builtin", configuration.exists(user.getName()))
                        .field("external_roles", user.getRoles())
                        .field("attributes", user.getCustomAttributesMap().keySet())
                        .field("roles", securityRoles);
            }
            builder.endObject();

            response = new BytesRestResponse(RestStatus.OK, builder);
        } catch (final Exception exception) {
            log.error(exception.toString(), exception);

            builder.startObject()
                    .field("error", exception.toString())
                    .endObject();

            response = new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
        }
        channel.sendResponse(response);
    }

    /**
     * PUT request to update account password.
     *
     * Sample request:
     * PUT _security/account
     * {
     *     "current_password": "old-pass",
     *     "password": "new-pass"
     * }
     *
     * Sample response:
     * {
     *     "status":"OK",
     *     "message":"'test' updated."
     * }
     *
     * @param channel channel to return response
     * @param request request to be served
     * @param client client
     * @param content content body
     * @throws IOException
     */
    @Override
    protected void handlePut(RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException {
        final User user = threadContext.getTransient(ConfigConstants.SECURITY_USER);
        final String username = user.getName();
        final SecurityDynamicConfiguration<?> internalUser = load(CType.USER, false);

        if (!internalUser.exists(username)) {
            notFound(channel, "Could not find user.");
            return;
        }

        if (!isWriteable(channel, internalUser, username)) {
            return;
        }

        final SecurityJsonNode securityJsonNode = new SecurityJsonNode(content);
        final String currentPassword = content.get("current_password").asText();
        final Hashed internalUserEntry = (Hashed) internalUser.getCEntry(username);
        final String currentHash = internalUserEntry.getHash();

        if (currentHash == null || !OpenBSDBCrypt.checkPassword(currentHash, currentPassword.toCharArray())) {
            badRequestResponse(channel, "Could not validate your current password.");
            return;
        }

        // if password is set, it takes precedence over hash
        final String password = securityJsonNode.get("password").asString();
        final String hash;
        if (Strings.isNullOrEmpty(password)) {
            hash = securityJsonNode.get("hash").asString();
        } else {
            hash = Utils.hash(password.toCharArray());
        }
        if (Strings.isNullOrEmpty(hash)) {
            badRequestResponse(channel, "Both provided password and hash cannot be null/empty.");
            return;
        }

        internalUserEntry.setHash(hash);

        saveAnUpdateConfigs(client, request, CType.USER, internalUser, new OnSucessActionListener<IndexResponse>(channel) {
            @Override
            public void onResponse(IndexResponse response) {
                successResponse(channel, "'" + username + "' updated.");
            }
        });
    }

    @Override
    protected AbstractConfigurationValidator getValidator(RestRequest request, BytesReference ref, Object... params) {
        final User user = threadContext.getTransient(ConfigConstants.SECURITY_USER);
        return new AccountValidator(request, ref, this.settings, user.getName());
    }

    @Override
    protected String getResourceName() {
        return RESOURCE_NAME;
    }

    @Override
    protected Endpoint getEndpoint() {
        return Endpoint.ACCOUNT;
    }

    @Override
    protected void filter(SecurityDynamicConfiguration<?> builder) {
        super.filter(builder);
        // replace password hashes in addition. We must not remove them from the
        // Builder since this would remove users completely if they
        // do not have any addition properties like roles or attributes
        builder.clearHashes();
    }

    @Override
    protected CType getConfigName() {
        return CType.USER;
    }
}
