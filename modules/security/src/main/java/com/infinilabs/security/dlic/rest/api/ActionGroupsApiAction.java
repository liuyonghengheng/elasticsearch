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

import java.nio.file.Path;
import java.util.List;

import com.infinilabs.security.auditlog.AuditLog;
import com.infinilabs.security.configuration.AdminDNs;
import com.infinilabs.security.privileges.PrivilegesEvaluator;
import com.infinilabs.security.securityconf.impl.CType;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.threadpool.ThreadPool;

import com.infinilabs.security.configuration.ConfigurationRepository;
import com.infinilabs.security.dlic.rest.validation.AbstractConfigurationValidator;
import com.infinilabs.security.dlic.rest.validation.ActionGroupValidator;
import com.infinilabs.security.ssl.transport.PrincipalExtractor;

import com.google.common.collect.ImmutableList;

public class ActionGroupsApiAction extends PatchableResourceApiAction {

	private static final List<Route> routes = ImmutableList.of(
			// corrected mapping, introduced in INFINI Elasticsearch Security
			new Route(Method.GET, "/_security/privilege/{name}"),
			new Route(Method.GET, "/_security/privilege/"),
			new Route(Method.DELETE, "/_security/privilege/{name}"),
			new Route(Method.PUT, "/_security/privilege/{name}"),
			new Route(Method.PATCH, "/_security/privilege/"),
			new Route(Method.PATCH, "/_security/privilege/{name}")
	);

	@Override
	protected Endpoint getEndpoint() {
		return Endpoint.PRIVILEGE;
	}

	@Inject
	public ActionGroupsApiAction(final Settings settings, final Path configPath, final RestController controller, final Client client,
                                 final AdminDNs adminDNs, final ConfigurationRepository cl, final ClusterService cs,
                                 final PrincipalExtractor principalExtractor, final PrivilegesEvaluator evaluator, ThreadPool threadPool, AuditLog auditLog) {
		super(settings, configPath, controller, client, adminDNs, cl, cs, principalExtractor, evaluator, threadPool, auditLog);
	}

	@Override
	public List<Route> routes() {
		return routes;
	}

	@Override
	protected AbstractConfigurationValidator getValidator(final RestRequest request, BytesReference ref, Object... param) {
		return new ActionGroupValidator(request, isSuperAdmin(), ref, this.settings, param);
	}

	@Override
	protected CType getConfigName() {
		return CType.PRIVILEGE;
	}

	@Override
    protected String getResourceName() {
        return "actiongroup";
	}

	@Override
	protected void consumeParameters(final RestRequest request) {
		request.param("name");
	}

}
