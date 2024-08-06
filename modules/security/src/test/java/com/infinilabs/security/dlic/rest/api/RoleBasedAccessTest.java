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
import com.infinilabs.security.support.SecurityJsonNode;
import com.infinilabs.security.test.helper.file.FileHelper;
import com.infinilabs.security.test.helper.rest.RestHelper;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

public class RoleBasedAccessTest extends AbstractRestApiUnitTest {

	@Test
	public void testActionGroupsApi() throws Exception {

		setupWithRestRoles();

		rh.sendAdminCertificate = false;

		// worf and sarek have access, worf has some endpoints disabled

		// ------ GET ------

		// --- Allowed Access ---

		// legacy user API, accessible for worf, single user
		RestHelper.HttpResponse response = rh.executeGetRequest("/_security/user/admin", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertTrue(settings.get("admin.hash") != null);
		Assert.assertEquals("", settings.get("admin.hash"));

		// new user API, accessible for worf, single user
		response = rh.executeGetRequest("/_security/user/admin", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		 settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertTrue(settings.get("admin.hash") != null);

		// legacy user API, accessible for worf, get complete config
		response = rh.executeGetRequest("/_security/user/", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals("", settings.get("admin.hash"));
		Assert.assertEquals("", settings.get("sarek.hash"));
		Assert.assertEquals("", settings.get("worf.hash"));

		// new user API, accessible for worf
		response = rh.executeGetRequest("/_security/user/", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals("", settings.get("admin.hash"));
		Assert.assertEquals("", settings.get("sarek.hash"));
		Assert.assertEquals("", settings.get("worf.hash"));

		// legacy user API, accessible for worf, get complete config, no trailing slash
		response = rh.executeGetRequest("/_security/user", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals("", settings.get("admin.hash"));
		Assert.assertEquals("", settings.get("sarek.hash"));
		Assert.assertEquals("", settings.get("worf.hash"));

		// new user API, accessible for worf, get complete config, no trailing slash
		response = rh.executeGetRequest("/_security/user", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals("", settings.get("admin.hash"));
		Assert.assertEquals("", settings.get("sarek.hash"));
		Assert.assertEquals("", settings.get("worf.hash"));

		// roles API, GET accessible for worf
		response = rh.executeGetRequest("/_security/role_mapping", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals("", settings.getAsList("security_superuser.users").get(0), "nagilum");
		Assert.assertEquals("", settings.getAsList("security_role_starfleet_library.external_roles").get(0), "starfleet*");
		Assert.assertEquals("", settings.getAsList("security_zdummy_all.users").get(0), "bug108");


		// Deprecated get configuration API, acessible for sarek
		// response = rh.executeGetRequest("_security/configuration/user", encodeBasicHeader("sarek", "sarek"));
		// settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		// Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		// Assert.assertEquals("", settings.get("admin.hash"));
		// Assert.assertEquals("", settings.get("sarek.hash"));
		// Assert.assertEquals("", settings.get("worf.hash"));

		// Deprecated get configuration API, acessible for sarek
		// response = rh.executeGetRequest("_security/configuration/privilege", encodeBasicHeader("sarek", "sarek"));
		// settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		// Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		// Assert.assertEquals("", settings.getAsList("ALL").get(0), "indices:*");
		// Assert.assertEquals("", settings.getAsList("SECURITY_CLUSTER_MONITOR").get(0), "cluster:monitor/*");
		// new format for action groups
		// Assert.assertEquals("", settings.getAsList("CRUD.permissions").get(0), "READ_UT");

		// configuration API, not accessible for worf
//		response = rh.executeGetRequest("_security/configuration/privilege", encodeBasicHeader("worf", "worf"));
//		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
//		Assert.assertTrue(response.getBody().contains("does not have any access to endpoint CONFIGURATION"));

		// cache API, not accessible for worf since it's disabled globally
		response = rh.executeDeleteRequest("_security/cache", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("does not have any access to endpoint CACHE"));

		// cache API, not accessible for sarek since it's disabled globally
		response = rh.executeDeleteRequest("_security/cache", encodeBasicHeader("sarek", "sarek"));
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("does not have any access to endpoint CACHE"));

		// Admin user has no eligible role at all
		response = rh.executeGetRequest("/_security/user/admin", encodeBasicHeader("admin", "admin"));
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("does not have any role privileged for admin access"));

		// Admin user has no eligible role at all
		response = rh.executeGetRequest("/_security/user/admin", encodeBasicHeader("admin", "admin"));
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("does not have any role privileged for admin access"));

		// Admin user has no eligible role at all
		response = rh.executeGetRequest("/_security/user", encodeBasicHeader("admin", "admin"));
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("does not have any role privileged for admin access"));

		// Admin user has no eligible role at all
		response = rh.executeGetRequest("/_security/role", encodeBasicHeader("admin", "admin"));
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("does not have any role privileged for admin access"));

		// --- DELETE ---

		// Admin user has no eligible role at all
		response = rh.executeDeleteRequest("/_security/user/admin", encodeBasicHeader("admin", "admin"));
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("does not have any role privileged for admin access"));

		// Worf, has access to user API, able to delete
		response = rh.executeDeleteRequest("/_security/user/other", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("'other' deleted"));

		// Worf, has access to user API, user "other" deleted now
		response = rh.executeGetRequest("/_security/user/other", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("'other' not found"));

		// Worf, has access to roles API, get captains role
		response = rh.executeGetRequest("/_security/role/security_role_starfleet_captains", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertEquals(new SecurityJsonNode(DefaultObjectMapper.readTree(response.getBody())).getDotted("security_role_starfleet_captains.cluster").get(0).asString(), "cluster:monitor*");

		// Worf, has access to roles API, able to delete
		response = rh.executeDeleteRequest("/_security/role/security_role_starfleet_captains", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("'security_role_starfleet_captains' deleted"));

		// Worf, has access to roles API, captains role deleted now
		response = rh.executeGetRequest("/_security/role/security_role_starfleet_captains", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("'security_role_starfleet_captains' not found"));

		// Worf, has no DELETE access to rolemappings API
		response = rh.executeDeleteRequest("/_security/role_mapping/security_unittest_1", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

		// Worf, has no DELETE access to rolemappings API, legacy endpoint
		response = rh.executeDeleteRequest("/_security/role_mapping/security_unittest_1", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

		// --- PUT ---

		// admin, no access
		response = rh.executePutRequest("/_security/role/security_role_starfleet_captains",
				FileHelper.loadFile("restapi/roles_captains_tenants.json"), encodeBasicHeader("admin", "admin"));
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

		// worf, restore role starfleet captains
		response = rh.executePutRequest("/_security/role/security_role_starfleet_captains",
				FileHelper.loadFile("restapi/roles_captains_different_content.json"), encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();

		// starfleet role present again
		response = rh.executeGetRequest("/_security/role/security_role_starfleet_captains", encodeBasicHeader("worf", "worf"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertEquals(new SecurityJsonNode(DefaultObjectMapper.readTree(response.getBody())).getDotted("security_role_starfleet_captains.indices").get(0).get("privileges").get(0).asString(), "blafasel");

		// Try the same, but now with admin certificate
		rh.sendAdminCertificate = true;

		// admin
		response = rh.executeGetRequest("/_security/user/admin", encodeBasicHeader("la", "lu"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertTrue(settings.get("admin.hash") != null);
		Assert.assertEquals("", settings.get("admin.hash"));

		// worf and config
		// response = rh.executeGetRequest("_security/configuration/privilege", encodeBasicHeader("bla", "fasel"));
		// Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// cache
		response = rh.executeDeleteRequest("_security/cache", encodeBasicHeader("wrong", "wrong"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// -- test user, does not have any endpoints disabled, but has access to API, i.e. full access

		rh.sendAdminCertificate = false;

		// GET privilege
		// response = rh.executeGetRequest("_security/configuration/privilege", encodeBasicHeader("test", "test"));
		// Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		response = rh.executeGetRequest("_security/privilege", encodeBasicHeader("test", "test"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// clear cache - globally disabled, has to fail
		response = rh.executeDeleteRequest("_security/cache", encodeBasicHeader("test", "test"));
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

		// PUT roles
		response = rh.executePutRequest("/_security/role/security_role_starfleet_captains",
				FileHelper.loadFile("restapi/roles_captains_different_content.json"), encodeBasicHeader("test", "test"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// GET captions role
		response = rh.executeGetRequest("/_security/role/security_role_starfleet_captains", encodeBasicHeader("test", "test"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// Delete captions role
		response = rh.executeDeleteRequest("/_security/role/security_role_starfleet_captains", encodeBasicHeader("test", "test"));
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("'security_role_starfleet_captains' deleted"));

		// GET captions role
		response = rh.executeGetRequest("/_security/role/security_role_starfleet_captains", encodeBasicHeader("test", "test"));
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());


	}
}
