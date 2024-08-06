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

import com.infinilabs.security.DefaultObjectMapper;
import com.infinilabs.security.test.helper.rest.RestHelper;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

public class GetConfigurationApiTest extends AbstractRestApiUnitTest {

	@Test
	public void testGetConfiguration() throws Exception {

		setup();
		rh.keystore = "restapi/kirk-keystore.jks";
		rh.sendAdminCertificate = true;

		// wrong config name -> bad request
		RestHelper.HttpResponse response = null;

		// test that every config is accessible
		// config
		response = rh.executeGetRequest("_security/securityconfig");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(
				settings.getAsBoolean("config.dynamic.authc.authentication_domain_basic_internal.http_enabled", false),
				true);
		Assert.assertNull(settings.get("_security_meta.type"));

		// user
		response = rh.executeGetRequest("_security/user");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals("", settings.get("admin.hash"));
		Assert.assertEquals("", settings.get("other.hash"));
		Assert.assertNull(settings.get("_security_meta.type"));

		// roles
		response = rh.executeGetRequest("_security/role");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		JsonNode jnode = DefaultObjectMapper.readTree(response.getBody());
		Assert.assertEquals(jnode.get("security_superuser").get("cluster").get(0).asText(), "cluster:*");
		Assert.assertNull(settings.get("_security_meta.type"));

		// roles
		response = rh.executeGetRequest("_security/role_mapping");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(settings.getAsList("security_role_starfleet.external_roles").get(0), "starfleet");
		Assert.assertNull(settings.get("_security_meta.type"));

		// action groups
		response = rh.executeGetRequest("_security/privilege");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(settings.getAsList("ALL.privileges").get(0), "indices:*");
		Assert.assertTrue(settings.hasValue("INTERNAL.privileges"));
		Assert.assertNull(settings.get("_security_meta.type"));
	}

}
