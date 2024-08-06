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

import java.util.List;

import com.infinilabs.security.dlic.rest.validation.AbstractConfigurationValidator;
import com.infinilabs.security.test.helper.file.FileHelper;
import com.infinilabs.security.test.helper.rest.RestHelper;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

public class RolesMappingApiTest extends AbstractRestApiUnitTest {

	@Test
	public void testRolesMappingApi() throws Exception {

		setup();

		rh.keystore = "restapi/kirk-keystore.jks";
		rh.sendAdminCertificate = true;

		// check role_mapping exists, old config api
		RestHelper.HttpResponse response = rh.executeGetRequest("_security/role_mapping");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// check role_mapping exists, new API
		response = rh.executeGetRequest("_security/role_mapping");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getContentType(), response.isJsonContentType());

		// Superadmin should be able to see hidden role_mapping
		Assert.assertTrue(response.getBody().contains("security_hidden"));

		// Superadmin should be able to see reserved role_mapping
		Assert.assertTrue(response.getBody().contains("security_reserved"));


		// -- GET

		// GET security_role_starfleet, exists
		response = rh.executeGetRequest("/_security/role_mapping/security_role_starfleet", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getContentType(), response.isJsonContentType());
		Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals("starfleet", settings.getAsList("security_role_starfleet.external_roles").get(0));
		Assert.assertEquals("captains", settings.getAsList("security_role_starfleet.external_roles").get(1));
		Assert.assertEquals("*.starfleetintranet.com", settings.getAsList("security_role_starfleet.hosts").get(0));
		Assert.assertEquals("nagilum", settings.getAsList("security_role_starfleet.users").get(0));

		// GET, role_mapping does not exist
		response = rh.executeGetRequest("/_security/role_mapping/nothinghthere", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

		// GET, new URL endpoint in security
		response = rh.executeGetRequest("/_security/role_mapping/", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getContentType(), response.isJsonContentType());

		// GET, new URL endpoint in security
		response = rh.executeGetRequest("/_security/role_mapping", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getContentType(), response.isJsonContentType());

		// Super admin should be able to describe particular hidden rolemapping
		response = rh.executeGetRequest("/_security/role_mapping/security_internal", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("\"hidden\":true"));

		// create index
		setupStarfleetIndex();

		// add user picard, role captains initially maps to
		// security_role_starfleet_captains and security_role_starfleet
		addUserWithPassword("picard", "picard", new String[] { "captains" }, HttpStatus.SC_CREATED);
		checkWriteAccess(HttpStatus.SC_CREATED, "picard", "picard", "sf", "ships", 1);

		// TODO: only one doctype allowed for ES6
		//checkWriteAccess(HttpStatus.SC_CREATED, "picard", "picard", "sf", "public", 1);

		// --- DELETE

		rh.sendAdminCertificate = true;

		// Non-existing role
		response = rh.executeDeleteRequest("/_security/role_mapping/idonotexist", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

		// read only role
		// SuperAdmin can delete read only role
		response = rh.executeDeleteRequest("/_security/role_mapping/security_role_starfleet_library", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// hidden role
        response = rh.executeDeleteRequest("/_security/role_mapping/security_internal", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Assert.assertTrue(response.getBody().contains("'security_internal' deleted."));

		// remove complete role mapping for security_role_starfleet_captains
		response = rh.executeDeleteRequest("/_security/role_mapping/security_role_starfleet_captains", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		response = rh.executeGetRequest("_security/configuration/role_mapping");
		rh.sendAdminCertificate = false;

		// now picard is only in security_role_starfleet, which has write access to
		// public, but not to ships
		checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "ships", 1);

		// TODO: only one doctype allowed for ES6
		// checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "public", 1);

		// remove also security_role_starfleet, poor picard has no mapping left
		rh.sendAdminCertificate = true;
		response = rh.executeDeleteRequest("/_security/role_mapping/security_role_starfleet", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		rh.sendAdminCertificate = false;
		checkAllSfForbidden();

		rh.sendAdminCertificate = true;

		// --- PUT

		// put with empty mapping, must fail
		response = rh.executePutRequest("/_security/role_mapping/security_role_starfleet_captains", "", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.PAYLOAD_MANDATORY.getMessage(), settings.get("reason"));

		// put new configuration with invalid payload, must fail
		response = rh.executePutRequest("/_security/role_mapping/security_role_starfleet_captains",
				FileHelper.loadFile("restapi/role_mapping_not_parseable.json"), new Header[0]);
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.BODY_NOT_PARSEABLE.getMessage(), settings.get("reason"));

		// put new configuration with invalid keys, must fail
		response = rh.executePutRequest("/_security/role_mapping/security_role_starfleet_captains",
				FileHelper.loadFile("restapi/role_mapping_invalid_keys.json"), new Header[0]);
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.INVALID_CONFIGURATION.getMessage(), settings.get("reason"));
		Assert.assertTrue(settings.get(AbstractConfigurationValidator.INVALID_KEYS_KEY + ".keys").contains("theusers"));
		Assert.assertTrue(
				settings.get(AbstractConfigurationValidator.INVALID_KEYS_KEY + ".keys").contains("thebackendroles"));
		Assert.assertTrue(settings.get(AbstractConfigurationValidator.INVALID_KEYS_KEY + ".keys").contains("thehosts"));

		// wrong datatypes
		response = rh.executePutRequest("/_security/role_mapping/security_role_starfleet_captains",
				FileHelper.loadFile("restapi/role_mapping_backendroles_captains_single_wrong_datatype.json"), new Header[0]);
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.WRONG_DATATYPE.getMessage(), settings.get("reason"));
		Assert.assertTrue(settings.get("external_roles").equals("Array expected"));
		Assert.assertTrue(settings.get("hosts") == null);
		Assert.assertTrue(settings.get("users") == null);

		response = rh.executePutRequest("/_security/role_mapping/security_role_starfleet_captains",
				FileHelper.loadFile("restapi/role_mapping_hosts_single_wrong_datatype.json"), new Header[0]);
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.WRONG_DATATYPE.getMessage(), settings.get("reason"));
		Assert.assertTrue(settings.get("hosts").equals("Array expected"));
		Assert.assertTrue(settings.get("external_roles") == null);
		Assert.assertTrue(settings.get("users") == null);

		response = rh.executePutRequest("/_security/role_mapping/security_role_starfleet_captains",
				FileHelper.loadFile("restapi/role_mapping_users_picard_single_wrong_datatype.json"), new Header[0]);
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.WRONG_DATATYPE.getMessage(), settings.get("reason"));
		Assert.assertTrue(settings.get("hosts").equals("Array expected"));
		Assert.assertTrue(settings.get("users").equals("Array expected"));
		Assert.assertTrue(settings.get("external_roles").equals("Array expected"));

		// Read only role mapping
		// SuperAdmin can add read only roles - mappings
		response = rh.executePutRequest("/_security/role_mapping/security_role_starfleet_library",
				FileHelper.loadFile("restapi/role_mapping_superuser.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

		// hidden role, allowed for super admin
        response = rh.executePutRequest("/_security/role_mapping/security_internal",
                FileHelper.loadFile("restapi/role_mapping_superuser.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

		response = rh.executePutRequest("/_security/role_mapping/security_role_starfleet_captains",
				FileHelper.loadFile("restapi/role_mapping_superuser.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

	    // -- PATCH
        // PATCH on non-existing resource
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role_mapping/imnothere", "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // PATCH read only resource, must be forbidden
		// SuperAdmin can patch read-only resource
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role_mapping/security_role_starfleet_library", "[{ \"op\": \"add\", \"path\": \"/description\", \"value\": \"foo\"] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        // PATCH hidden resource, must be not found, can be found by super admin
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role_mapping/security_internal", "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ " +
            "\"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        // PATCH value of hidden flag, must fail with validation error
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role_mapping/security_role_vulcans", "[{ \"op\": \"add\", \"path\": \"/hidden\", \"value\": true }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));

        // PATCH
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role_mapping/security_role_vulcans", "[{ \"op\": \"add\", \"path\": \"/external_roles/-\", \"value\": \"spring\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_security/role_mapping/security_role_vulcans", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
        List<String> permissions = settings.getAsList("security_role_vulcans.external_roles");
        Assert.assertNotNull(permissions);
        Assert.assertTrue(permissions.contains("spring"));

        // -- PATCH on whole config resource
        // PATCH on non-existing resource
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role_mapping", "[{ \"op\": \"add\", \"path\": \"/imnothere/a\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        // PATCH read only resource, must be forbidden
		// SuperAdmin can patch read only resource
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role_mapping", "[{ \"op\": \"add\", \"path\": \"/security_role_starfleet_library/description\", \"value\": \"foo\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        // PATCH hidden resource, must be bad request
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role_mapping", "[{ \"op\": \"add\", \"path\": \"/security_internal/a\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        // PATCH value of hidden flag, must fail with validation error
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role_mapping", "[{ \"op\": \"add\", \"path\": \"/security_role_vulcans/hidden\", \"value\": true }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));

        // PATCH
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role_mapping", "[{ \"op\": \"add\", \"path\": \"/bulknew1\", \"value\": {  \"external_roles\":[\"vulcanadmin\"]} }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_security/role_mapping/bulknew1", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
        permissions = settings.getAsList("bulknew1.external_roles");
        Assert.assertNotNull(permissions);
        Assert.assertTrue(permissions.contains("vulcanadmin"));

        // PATCH delete
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role_mapping", "[{ \"op\": \"remove\", \"path\": \"/bulknew1\"}]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_security/role_mapping/bulknew1", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());


		// mapping with several backend roles, one of the is captain
		deleteAndputNewMapping("role_mapping_backendroles_captains_list.json");
		checkAllSfAllowed();

		// mapping with one backend role, captain
		deleteAndputNewMapping("role_mapping_backendroles_captains_single.json");
		checkAllSfAllowed();

		// mapping with several users, one is picard
		deleteAndputNewMapping("role_mapping_users_picard_list.json");
		checkAllSfAllowed();

		// just user picard
		deleteAndputNewMapping("role_mapping_users_picard_single.json");
		checkAllSfAllowed();

		// hosts
		deleteAndputNewMapping("role_mapping_hosts_list.json");
		checkAllSfAllowed();

		// hosts
		deleteAndputNewMapping("role_mapping_hosts_single.json");
		checkAllSfAllowed();

		// full settings, access
		deleteAndputNewMapping("role_mapping_superuser.json");
		checkAllSfAllowed();

		// full settings, no access
		deleteAndputNewMapping("role_mapping_all_noaccess.json");
		checkAllSfForbidden();

	}

	private void checkAllSfAllowed() throws Exception {
		rh.sendAdminCertificate = false;
		checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 1);
		checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 1);
		// ES7 only supports one doc type, so trying to create a second one leads to 400  BAD REQUEST
		checkWriteAccess(HttpStatus.SC_BAD_REQUEST, "picard", "picard", "sf", "public", 1);
	}

	private void checkAllSfForbidden() throws Exception {
		rh.sendAdminCertificate = false;
		checkReadAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "ships", 1);
		checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "ships", 1);
	}

	private RestHelper.HttpResponse deleteAndputNewMapping(String fileName) throws Exception {
		rh.sendAdminCertificate = true;
		RestHelper.HttpResponse response = rh.executeDeleteRequest("/_security/role_mapping/security_role_starfleet_captains",
				new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		response = rh.executePutRequest("/_security/role_mapping/security_role_starfleet_captains",
				FileHelper.loadFile("restapi/"+fileName), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
		rh.sendAdminCertificate = false;
		return response;
	}

	@Test
	public void testRolesMappingApiForNonSuperAdmin() throws Exception {

		setupWithRestRoles();

		rh.keystore = "restapi/kirk-keystore.jks";
		rh.sendAdminCertificate = false;
		rh.sendHTTPClientCredentials = true;

		RestHelper.HttpResponse response;

		// Delete read only roles mapping
		response = rh.executeDeleteRequest("/_security/role_mapping/security_role_starfleet_library" , new Header[0]);
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

		// Put read only roles mapping
		response = rh.executePutRequest("/_security/role_mapping/security_role_starfleet_library",
				FileHelper.loadFile("restapi/role_mapping_superuser.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

		// Patch single read only roles mapping
		response = rh.executePatchRequest("/_security/role_mapping/security_role_starfleet_library", "[{ \"op\": \"add\", \"path\": \"/description\", \"value\": \"foo\" }]", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

		// Patch multiple read only roles mapping
		response = rh.executePatchRequest("/_security/role_mapping", "[{ \"op\": \"add\", \"path\": \"/security_role_starfleet_library/description\", \"value\": \"foo\" }]", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

		// GET, role_mapping is hidden, allowed for super admin
		response = rh.executeGetRequest("/_security/role_mapping/security_internal", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

		// Delete hidden roles mapping
		response = rh.executeDeleteRequest("/_security/role_mapping/security_internal" , new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

		// Put hidden roles mapping
		response = rh.executePutRequest("/_security/role_mapping/security_internal",
				FileHelper.loadFile("restapi/role_mapping_superuser.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

		// Patch hidden roles mapping
		response = rh.executePatchRequest("/_security/role_mapping/security_internal", "[{ \"op\": \"add\", \"path\": \"/description\", \"value\": \"foo\" }]", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

		// Patch multiple hidden roles mapping
		response = rh.executePatchRequest("/_security/role_mapping", "[{ \"op\": \"add\", \"path\": \"/security_internal/description\", \"value\": \"foo\" }]", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

	}
}
