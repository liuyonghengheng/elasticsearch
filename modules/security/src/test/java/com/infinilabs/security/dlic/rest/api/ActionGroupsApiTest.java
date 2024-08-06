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

public class ActionGroupsApiTest extends AbstractRestApiUnitTest {

	@Test
	public void testActionGroupsApi() throws Exception {

		setup();

		rh.keystore = "restapi/kirk-keystore.jks";
		rh.sendAdminCertificate = true;

		// --- GET_UT
        // GET_UT, actiongroup exists
        RestHelper.HttpResponse response = rh.executeGetRequest("/_security/privilege/CRUD_UT", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		Settings settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
        List<String> permissions = settings.getAsList("CRUD_UT.privileges");
		Assert.assertNotNull(permissions);
		Assert.assertEquals(2, permissions.size());
        Assert.assertTrue(permissions.contains("READ_UT"));
		Assert.assertTrue(permissions.contains("SECURITY_WRITE"));

		// GET_UT, actiongroup does not exist
		response = rh.executeGetRequest("/_security/privilege/nothinghthere", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

		// GET_UT, old endpoint
		response = rh.executeGetRequest("/_security/privilege/", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// GET_UT, old endpoint
		response = rh.executeGetRequest("/_security/actiongroup", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// GET_UT, new endpoint which replaces configuration endpoint
		response = rh.executeGetRequest("/_security/privilege/", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// GET_UT, old endpoint
		response = rh.executeGetRequest("/_security/privilege", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// GET_UT, new endpoint which replaces configuration endpoint
		response = rh.executeGetRequest("/_security/privilege/", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// GET_UT, new endpoint which replaces configuration endpoint
		response = rh.executeGetRequest("/_security/privilege", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// create index
		setupStarfleetIndex();

		// add user picard, role starfleet, maps to security_role_starfleet
		addUserWithPassword("picard", "picard", new String[] { "starfleet" }, HttpStatus.SC_CREATED);
		checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 0);
		// TODO: only one doctype allowed for ES6
		// checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "public", 0);
		checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "ships", 0);
		// TODO: only one doctype allowed for ES6
		//checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "public", 0);

		// -- DELETE
		// Non-existing role
		rh.sendAdminCertificate = true;

		response = rh.executeDeleteRequest("/_security/privilege/idonotexist", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

		// remove action group READ_UT, read access not possible since
		// security_role_starfleet
		// uses this action group.
		response = rh.executeDeleteRequest("/_security/privilege/READ_UT", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		rh.sendAdminCertificate = false;
		checkReadAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "ships", 0);

		// put picard in captains role. Role security_role_captains uses the CRUD_UT
		// action group
		// which uses READ_UT and WRITE action groups. We removed READ_UT, so only
		// WRITE is possible
		addUserWithPassword("picard", "picard", new String[] { "captains" }, HttpStatus.SC_OK);
		checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 0);
		checkReadAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "ships", 0);

		// now remove also CRUD_UT groups, write also not possible anymore
		rh.sendAdminCertificate = true;
		response = rh.executeDeleteRequest("/_security/privilege/CRUD_UT", new Header[0]);
		rh.sendAdminCertificate = false;
		checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "ships", 0);
		checkReadAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "ships", 0);

		// -- PUT

		// put with empty payload, must fail
		rh.sendAdminCertificate = true;
		response = rh.executePutRequest("/_security/privilege/SOMEGROUP", "", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.PAYLOAD_MANDATORY.getMessage(), settings.get("reason"));

		// put new configuration with invalid payload, must fail
		response = rh.executePutRequest("/_security/privilege/SOMEGROUP", FileHelper.loadFile("restapi/actiongroup_not_parseable.json"),
				new Header[0]);
		settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
		Assert.assertEquals(AbstractConfigurationValidator.ErrorType.BODY_NOT_PARSEABLE.getMessage(), settings.get("reason"));

		response = rh.executePutRequest("/_security/privilege/CRUD_UT", FileHelper.loadFile("restapi/actiongroup_crud.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

		rh.sendAdminCertificate = false;

		// write access allowed again, read forbidden, since READ_UT group is still missing
		checkReadAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "ships", 0);
		checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 0);

		// restore READ_UT action groups
		rh.sendAdminCertificate = true;
		response = rh.executePutRequest("/_security/privilege/READ_UT", FileHelper.loadFile("restapi/actiongroup_read.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

		rh.sendAdminCertificate = false;
		// read/write allowed again
		checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 0);
		checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 0);

		// -- PUT, new JSON format including readonly flag, disallowed in REST API
		rh.sendAdminCertificate = true;
		response = rh.executePutRequest("/_security/privilege/CRUD_UT", FileHelper.loadFile("restapi/actiongroup_readonly.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// -- DELETE read only resource, must be forbidden
        // superAdmin can delete read only resource
		rh.sendAdminCertificate = true;
		response = rh.executeDeleteRequest("/_security/privilege/GET_UT", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

		// -- PUT read only resource, must be forbidden
        // superAdmin can add/update read only resource
		rh.sendAdminCertificate = true;
        response = rh.executePutRequest("/_security/privilege/GET_UT", FileHelper.loadFile("restapi/actiongroup_read.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
        Assert.assertFalse(response.getBody().contains("Resource 'GET_UT' is read-only."));

        // -- GET_UT hidden resource, must be 404 but super admin can find it
        rh.sendAdminCertificate = true;
        response = rh.executeGetRequest("/_security/privilege/INTERNAL", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("\"hidden\":true"));

		// -- DELETE hidden resource, must be 404
        rh.sendAdminCertificate = true;
        response = rh.executeDeleteRequest("/_security/privilege/INTERNAL", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("'INTERNAL' deleted."));

        // -- PUT hidden resource, must be forbidden
        rh.sendAdminCertificate = true;
        response = rh.executePutRequest("/_security/privilege/INTERNAL", FileHelper.loadFile("restapi/actiongroup_read.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

        // -- PATCH
        // PATCH on non-existing resource
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/privilege/imnothere", "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // PATCH read only resource, must be forbidden
        // SuperAdmin can patch read only resource
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/privilege/GET_UT", "[{ \"op\": \"add\", \"path\": \"/description\", \"value\": \"foo\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        // PATCH hidden resource, must be not found, can be found by superadmin, but fails with no path exist error
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/privilege/INTERNAL", "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        // PATCH value of hidden flag, must fail with validation error
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/privilege/CRUD_UT", "[{ \"op\": \"add\", \"path\": \"/hidden\", \"value\": true }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody(), response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));

        // PATCH with relative JSON pointer, must fail
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/privilege/CRUD_UT", "[{ \"op\": \"add\", \"path\": \"1/INTERNAL/privileges/-\", \"value\": \"SECURITY_DELETE\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        // PATCH new format
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/privilege/CRUD_UT", "[{ \"op\": \"add\", \"path\": \"/privileges/-\", \"value\": \"SECURITY_DELETE\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_security/privilege/CRUD_UT", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
        permissions = settings.getAsList("CRUD_UT.privileges");
        Assert.assertNotNull(permissions);
        Assert.assertEquals(3, permissions.size());
        Assert.assertTrue(permissions.contains("READ_UT"));
        Assert.assertTrue(permissions.contains("SECURITY_WRITE"));
        Assert.assertTrue(permissions.contains("SECURITY_DELETE"));


        // -- PATCH on whole config resource
        // PATCH read only resource, must be forbidden
        // SuperAdmin can patch read only resource
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/privilege", "[{ \"op\": \"add\", \"path\": \"/GET_UT/a\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/privilege", "[{ \"op\": \"add\", \"path\": \"/GET_UT/description\", \"value\": \"foo\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        // PATCH hidden resource, must be bad request
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/privilege", "[{ \"op\": \"add\", \"path\": \"/INTERNAL/a\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        // PATCH delete read only resource, must be forbidden
        // SuperAdmin can delete read only resource
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/privilege", "[{ \"op\": \"remove\", \"path\": \"/GET_UT\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        // PATCH delete hidden resource, must be bad request
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/privilege", "[{ \"op\": \"remove\", \"path\": \"/INTERNAL\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("\"message\":\"Resource updated."));


        // PATCH value of hidden flag, must fail with validation error
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/privilege", "[{ \"op\": \"add\", \"path\": \"/CRUD_UT/hidden\", \"value\": true }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));

        // add new resource with hidden flag, must fail with validation error
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/privilege", "[{ \"op\": \"add\", \"path\": \"/NEWNEWNEW\", \"value\": {\"privileges\": [\"indices:data/write*\"], \"hidden\":true }}]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));

        // add new valid resources
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/privilege", "[{ \"op\": \"add\", \"path\": \"/BULKNEW1\", \"value\": {\"privileges\": [\"indices:data/*\", \"cluster:monitor/*\"] } }," + "{ \"op\": \"add\", \"path\": \"/BULKNEW2\", \"value\": {\"privileges\": [\"READ_UT\"] } }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_security/privilege/BULKNEW1", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
        permissions = settings.getAsList("BULKNEW1.privileges");
        Assert.assertNotNull(permissions);
        Assert.assertEquals(2, permissions.size());
        Assert.assertTrue(permissions.contains("indices:data/*"));
        Assert.assertTrue(permissions.contains("cluster:monitor/*"));

        response = rh.executeGetRequest("/_security/privilege/BULKNEW2", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
        permissions = settings.getAsList("BULKNEW2.privileges");
        Assert.assertNotNull(permissions);
        Assert.assertEquals(1, permissions.size());
        Assert.assertTrue(permissions.contains("READ_UT"));

        // delete resource
        response = rh.executePatchRequest("/_security/privilege", "[{ \"op\": \"remove\", \"path\": \"/BULKNEW1\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_security/privilege/BULKNEW1", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // assert other resource is still there
        response = rh.executeGetRequest("/_security/privilege/BULKNEW2", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = Settings.builder().loadFromSource(response.getBody(), XContentType.JSON).build();
        permissions = settings.getAsList("BULKNEW2.privileges");
        Assert.assertNotNull(permissions);
        Assert.assertEquals(1, permissions.size());
        Assert.assertTrue(permissions.contains("READ_UT"));
	}

    @Test
    public void testActionGroupsApiForNonSuperAdmin() throws Exception {

        setupWithRestRoles();

        rh.keystore = "restapi/kirk-keystore.jks";
        rh.sendAdminCertificate = false;
        rh.sendHTTPClientCredentials = true;

        RestHelper.HttpResponse response;

        // Delete read only privilege
        response = rh.executeDeleteRequest("/_security/privilege/create_index" , new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        // Put read only privilege
        response = rh.executePutRequest("/_security/privilege/create_index", FileHelper.loadFile("restapi/actiongroup_crud.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        // Patch single read only privilege
        response = rh.executePatchRequest("/_security/privilege/create_index", "[{ \"op\": \"replace\", \"path\": \"/description\", \"value\": \"foo\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        // Patch multiple read only privilege
        response = rh.executePatchRequest("/_security/privilege", "[{ \"op\": \"replace\", \"path\": \"/create_index/description\", \"value\": \"foo\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        response = rh.executeGetRequest("/_security/privilege/INTERNAL" , new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // Delete hidden privilege
        response = rh.executeDeleteRequest("/_security/privilege/INTERNAL" , new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // Put hidden privilege
        response = rh.executePutRequest("/_security/privilege/INTERNAL", FileHelper.loadFile("restapi/actiongroup_crud.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // Patch hidden privilege
        response = rh.executePatchRequest("/_security/privilege/INTERNAL", "[{ \"op\": \"replace\", \"path\": \"/description\", \"value\": \"foo\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // Patch multiple hidden privilege
        response = rh.executePatchRequest("/_security/privilege", "[{ \"op\": \"replace\", \"path\": \"/INTERNAL/description\", \"value\": \"foo\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

    }

}
