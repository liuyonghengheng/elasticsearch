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
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;


public class IndexMissingTest extends AbstractRestApiUnitTest {

	@Test
	public void testGetConfiguration() throws Exception {
	    // don't setup index for this test
	    init = false;
		setup();

		// test with no Security index at all
		testHttpOperations();

	}

	protected void testHttpOperations() throws Exception {

		rh.keystore = "restapi/kirk-keystore.jks";
		rh.sendAdminCertificate = true;

		// GET configuration
		RestHelper.HttpResponse response = rh.executeGetRequest("_security/role");
		Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatusCode());
		String errorString = response.getBody();
		System.out.println(errorString);
		Assert.assertEquals("{\"status\":\"INTERNAL_SERVER_ERROR\",\"message\":\"Security index not initialized\"}", errorString);

		// GET roles
		response = rh.executeGetRequest("/_security/role/security_role_starfleet", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatusCode());
		errorString = response.getBody();
        Assert.assertEquals("{\"status\":\"INTERNAL_SERVER_ERROR\",\"message\":\"Security index not initialized\"}", errorString);

		// GET role_mapping
		response = rh.executeGetRequest("/_security/role_mapping/security_role_starfleet", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatusCode());
		errorString = response.getBody();
		Assert.assertEquals("{\"status\":\"INTERNAL_SERVER_ERROR\",\"message\":\"Security index not initialized\"}", errorString);

		// GET privilege
		response = rh.executeGetRequest("_security/privilege/READ");
		Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatusCode());
		errorString = response.getBody();
		Assert.assertEquals("{\"status\":\"INTERNAL_SERVER_ERROR\",\"message\":\"Security index not initialized\"}", errorString);

		// GET user
		response = rh.executeGetRequest("_security/user/picard");
		Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatusCode());
		errorString = response.getBody();
		Assert.assertEquals("{\"status\":\"INTERNAL_SERVER_ERROR\",\"message\":\"Security index not initialized\"}", errorString);

		// PUT request
		response = rh.executePutRequest("/_security/privilege/READ", FileHelper.loadFile("restapi/actiongroup_read.json"), new Header[0]);
		Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatusCode());

		// DELETE request
		response = rh.executeDeleteRequest("/_security/role/security_role_starfleet", new Header[0]);
		Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatusCode());

		// setup index now
		initialize(clusterHelper, this.clusterInfo);

		// GET configuration
		response = rh.executeGetRequest("_security/role");
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
		SecurityJsonNode securityJsonNode = new SecurityJsonNode(DefaultObjectMapper.readTree(response.getBody()));
		Assert.assertEquals("SECURITY_CLUSTER_ALL", securityJsonNode.get("security_admin").get("cluster").get(0).asString());

	}
}
