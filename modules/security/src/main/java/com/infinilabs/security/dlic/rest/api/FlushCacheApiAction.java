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

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import com.infinilabs.security.action.configupdate.ConfigUpdateAction;
import com.infinilabs.security.action.configupdate.ConfigUpdateRequest;
import com.infinilabs.security.action.configupdate.ConfigUpdateResponse;
import com.infinilabs.security.auditlog.AuditLog;
import com.infinilabs.security.configuration.AdminDNs;
import com.infinilabs.security.dlic.rest.validation.AbstractConfigurationValidator;
import com.infinilabs.security.dlic.rest.validation.NoOpValidator;
import com.infinilabs.security.privileges.PrivilegesEvaluator;
import com.infinilabs.security.securityconf.impl.CType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.threadpool.ThreadPool;

import com.fasterxml.jackson.databind.JsonNode;
import com.infinilabs.security.configuration.ConfigurationRepository;
import com.infinilabs.security.ssl.transport.PrincipalExtractor;

import com.google.common.collect.ImmutableList;

public class FlushCacheApiAction extends AbstractApiAction {
	private static final List<Route> routes = ImmutableList.of(
			new Route(Method.DELETE, "/_security/cache"),
			new Route(Method.GET, "/_security/cache"),
			new Route(Method.PUT, "/_security/cache"),
			new Route(Method.POST, "/_security/cache")
	);

	@Inject
	public FlushCacheApiAction(final Settings settings, final Path configPath, final RestController controller, final Client client,
                               final AdminDNs adminDNs, final ConfigurationRepository cl, final ClusterService cs,
                               final PrincipalExtractor principalExtractor, final PrivilegesEvaluator evaluator, ThreadPool threadPool, AuditLog auditLog) {
		super(settings, configPath, controller, client, adminDNs, cl, cs, principalExtractor, evaluator, threadPool, auditLog);
	}

	@Override
	public List<Route> routes() {
		return routes;
	}

	@Override
	protected Endpoint getEndpoint() {
		return Endpoint.CACHE;
	}

	@Override
	protected void handleDelete(RestChannel channel,
	        RestRequest request, Client client, final JsonNode content) throws IOException
	{

		client.execute(
				ConfigUpdateAction.INSTANCE,
				new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0])),
				new ActionListener<ConfigUpdateResponse>() {

					@Override
					public void onResponse(ConfigUpdateResponse ur) {
					    if(ur.hasFailures()) {
					        log.error("Cannot flush cache due to", ur.failures().get(0));
	                        internalErrorResponse(channel, "Cannot flush cache due to "+ ur.failures().get(0).getMessage()+".");
	                        return;
	                    }
						successResponse(channel, "Cache flushed successfully.");
						if (log.isDebugEnabled()) {
						    log.debug("cache flushed successfully");
						}
					}

					@Override
					public void onFailure(Exception e) {
					    log.error("Cannot flush cache due to", e);
						internalErrorResponse(channel, "Cannot flush cache due to "+ e.getMessage()+".");
					}

				}
		);
	}

	@Override
	protected void handlePost(RestChannel channel, final RestRequest request, final Client client, final JsonNode content)throws IOException {
		notImplemented(channel, Method.POST);
	}

	@Override
	protected void handleGet(RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException{
		notImplemented(channel, Method.GET);
	}

	@Override
	protected void handlePut(RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException{
		notImplemented(channel, Method.PUT);
	}

	@Override
	protected AbstractConfigurationValidator getValidator(RestRequest request, BytesReference ref, Object... param) {
		return new NoOpValidator(request, ref, this.settings, param);
	}

	@Override
	protected String getResourceName() {
		// not needed
		return null;
	}

	@Override
    protected CType getConfigName() {
		return null;
	}

	@Override
	protected void consumeParameters(final RestRequest request) {
		// not needed
	}
}
