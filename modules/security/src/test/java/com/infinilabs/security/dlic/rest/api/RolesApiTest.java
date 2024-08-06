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

import com.infinilabs.security.DefaultObjectMapper;
import com.infinilabs.security.dlic.rest.validation.AbstractConfigurationValidator;
import com.infinilabs.security.support.SecurityJsonNode;
import com.infinilabs.security.test.helper.file.FileHelper;
import com.infinilabs.security.test.helper.rest.RestHelper;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

public class RolesApiTest extends AbstractRestApiUnitTest {

    @Test
    public void testAllSettings() throws Exception {
        setup();

        rh.keystore = "restapi/kirk-keystore.jks";
        rh.sendAdminCertificate = true;

        String json = "{\n" +
            "    \"index.search.slowlog.threshold.query.warn\": \"10s\",\n" +
            "    \"index.search.slowlog.threshold.query.info\": \"5s\",\n" +
            "    \"index.search.slowlog.threshold.query.debug\": \"2s\",\n" +
            "    \"index.search.slowlog.threshold.query.trace\": \"500ms\",\n" +
            "    \"index.search.slowlog.threshold.fetch.warn\": \"1s\",\n" +
            "    \"index.search.slowlog.threshold.fetch.info\": \"800ms\",\n" +
            "    \"index.search.slowlog.threshold.fetch.debug\": \"500ms\",\n" +
            "    \"index.search.slowlog.threshold.fetch.trace\": \"200ms\"\n" +
            "}";

        RestHelper.HttpResponse response = rh.executePutRequest("_all/_settings", json);
        System.out.println(response.getBody());
//        response = rh.executeGetRequest("_all/_settings");
//        System.out.println(response.getBody());

    }

    @Test
    public void testPutRole() throws Exception {

        setup();

        rh.keystore = "restapi/kirk-keystore.jks";
        rh.sendAdminCertificate = true;
        // check roles exists
        RestHelper.HttpResponse response = rh.executePutRequest("/_security/role/admin", FileHelper.loadFile("restapi/simple_role.json"));
        System.out.println(response.getBody());
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

        response = rh.executePutRequest("/_security/role/lala", "{ \"cluster\": [\"*\"] }");
        System.out.println(response.getBody());
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

        response = rh.executePutRequest("/_security/role/empty", "{ \"cluster\": [] }");
        System.out.println(response.getBody());
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
    }

    @Test
    public void testAllRolesForSuperAdmin() throws Exception {

        setup();

        rh.keystore = "restapi/kirk-keystore.jks";
        rh.sendAdminCertificate = true;
        RestHelper.HttpResponse response = rh.executeGetRequest("_security/role");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertFalse(response.getBody().contains("_meta"));

        // Super admin should be able to see all roles including hidden
        Assert.assertTrue(response.getBody().contains("security_hidden"));
    }

    @Test
    public void testPutDuplicateKeys() throws Exception {

        setup();

        rh.keystore = "restapi/kirk-keystore.jks";
        rh.sendAdminCertificate = true;
        RestHelper.HttpResponse response = rh.executePutRequest("_security/role/dup", "{ \"cluster\": [\"*\"], \"cluster\": [\"*\"] }");
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        assertHealthy();
    }

    @Test
    public void testPutUnknownKey() throws Exception {

        setup();

        rh.keystore = "restapi/kirk-keystore.jks";
        rh.sendAdminCertificate = true;
        RestHelper.HttpResponse response = rh.executePutRequest("_security/role/dup", "{ \"unknownkey\": [\"*\"], \"cluster\": [\"*\"] }");
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("invalid_keys"));
        assertHealthy();
    }

    @Test
    public void testPutInvalidJson() throws Exception {

        setup();

        rh.keystore = "restapi/kirk-keystore.jks";
        rh.sendAdminCertificate = true;
        RestHelper.HttpResponse response = rh.executePutRequest("_security/role/dup", "{ \"invalid\"::{{ [\"*\"], \"cluster\": [\"*\"] }");
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        assertHealthy();
    }

    @Test
    public void testRolesApi() throws Exception {

        setup();

        rh.keystore = "restapi/kirk-keystore.jks";
        rh.sendAdminCertificate = true;

        // check roles exists
        RestHelper.HttpResponse response = rh.executeGetRequest("/_security/role");
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        // -- GET

        // GET security_role_starfleet
        response = rh.executeGetRequest("/_security/role/security_role_starfleet", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        JsonNode settings = DefaultObjectMapper.readTree(response.getBody());
        Assert.assertEquals(1, settings.size());

        // GET, role does not exist
        response = rh.executeGetRequest("/_security/role/nothinghthere", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        response = rh.executeGetRequest("/_security/role/", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        response = rh.executeGetRequest("/_security/role", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("\"cluster\":[\"*\"]"));
        Assert.assertFalse(response.getBody().contains("\"cluster\" : ["));

        response = rh.executeGetRequest("/_security/role?pretty", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertFalse(response.getBody().contains("\"cluster\":[\"*\"]"));
        Assert.assertTrue(response.getBody().contains("\"cluster\" : ["));

        // Super admin should be able to describe hidden role
        response = rh.executeGetRequest("/_security/role/security_hidden", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("\"hidden\":true"));

        // create index
        setupStarfleetIndex();

        // add user picard, role starfleet, maps to security_role_starfleet
        addUserWithPassword("picard", "picard", new String[] { "starfleet", "captains" }, HttpStatus.SC_CREATED);
        checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 0);
        checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 0);

        // ES7 only supports one doc type, so trying to create a second one leads to 400  BAD REQUEST
        checkWriteAccess(HttpStatus.SC_BAD_REQUEST, "picard", "picard", "sf", "public", 0);


        // -- DELETE

        rh.sendAdminCertificate = true;

        // Non-existing role
        response = rh.executeDeleteRequest("/_security/role/idonotexist", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // read only role, SuperAdmin can delete the read-only role
        response = rh.executeDeleteRequest("/_security/role/security_transport_client", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        // hidden role allowed for superadmin
        response = rh.executeDeleteRequest("/_security/role/security_internal", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("'security_internal' deleted."));

        // remove complete role mapping for security_role_starfleet_captains
        response = rh.executeDeleteRequest("/_security/role/security_role_starfleet_captains", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        rh.sendAdminCertificate = false;

        // user has only role starfleet left, role has READ access only
        checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "ships", 1);

        // ES7 only supports one doc type, but Opendistro permission checks run first
        // So we also get a 403 FORBIDDEN when tring to add new document type
        checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "public", 0);

        rh.sendAdminCertificate = true;
        // remove also starfleet role, nothing is allowed anymore
        response = rh.executeDeleteRequest("/_security/role/security_role_starfleet", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        checkReadAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "ships", 0);
        checkWriteAccess(HttpStatus.SC_FORBIDDEN, "picard", "picard", "sf", "ships", 0);

        // -- PUT
        // put with empty roles, must fail
        response = rh.executePutRequest("/_security/role/security_role_starfleet", "", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        settings = DefaultObjectMapper.readTree(response.getBody());
        Assert.assertEquals(AbstractConfigurationValidator.ErrorType.PAYLOAD_MANDATORY.getMessage(), settings.get("reason").asText());

        // put new configuration with invalid payload, must fail
        response = rh.executePutRequest("/_security/role/security_role_starfleet",
                FileHelper.loadFile("restapi/roles_not_parseable.json"), new Header[0]);
        settings = DefaultObjectMapper.readTree(response.getBody());
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertEquals(AbstractConfigurationValidator.ErrorType.BODY_NOT_PARSEABLE.getMessage(), settings.get("reason").asText());

        // put new configuration with invalid keys, must fail
        response = rh.executePutRequest("/_security/role/security_role_starfleet",
                FileHelper.loadFile("restapi/roles_invalid_keys.json"), new Header[0]);
        settings = DefaultObjectMapper.readTree(response.getBody());
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertEquals(AbstractConfigurationValidator.ErrorType.INVALID_CONFIGURATION.getMessage(), settings.get("reason").asText());
        Assert.assertTrue(settings.get(AbstractConfigurationValidator.INVALID_KEYS_KEY).get("keys").asText().contains("indexx_permissions"));
        Assert.assertTrue(
                settings.get(AbstractConfigurationValidator.INVALID_KEYS_KEY).get("keys").asText().contains("kluster_permissions"));

        // put new configuration with wrong datatypes, must fail
        response = rh.executePutRequest("/_security/role/security_role_starfleet",
                FileHelper.loadFile("restapi/roles_wrong_datatype.json"), new Header[0]);
        settings = DefaultObjectMapper.readTree(response.getBody());
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertEquals(AbstractConfigurationValidator.ErrorType.WRONG_DATATYPE.getMessage(), settings.get("reason").asText());
        Assert.assertTrue(settings.get("cluster").asText().equals("Array expected"));

        // put read only role, must be forbidden
        // But SuperAdmin can still create it
        response = rh.executePutRequest("/_security/role/security_transport_client",
                FileHelper.loadFile("restapi/roles_captains.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

        // put hidden role, must be forbidden, but allowed for super admin
        response = rh.executePutRequest("/_security/role/security_internal",
                FileHelper.loadFile("restapi/roles_captains.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());

        // restore starfleet role
        response = rh.executePutRequest("/_security/role/security_role_starfleet",
                FileHelper.loadFile("restapi/roles_starfleet.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
        rh.sendAdminCertificate = false;
        checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 0);

        // now picard is only in security_role_starfleet, which has write access to
        // all indices. We collapse all document types in ODFE7 so this permission in the
        // starfleet role grants all permissions:
        //   public:
        //       - 'indices:*'
        checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 0);
        // ES7 only supports one doc type, so trying to create a second one leads to 400  BAD REQUEST
        checkWriteAccess(HttpStatus.SC_BAD_REQUEST, "picard", "picard", "sf", "public", 0);

        rh.sendAdminCertificate = true;

        // restore captains role
        response = rh.executePutRequest("/_security/role/security_role_starfleet_captains",
                FileHelper.loadFile("restapi/roles_captains.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
        rh.sendAdminCertificate = false;
        checkReadAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 0);
        checkWriteAccess(HttpStatus.SC_OK, "picard", "picard", "sf", "ships", 0);

        // ES7 only supports one doc type, so trying to create a second one leads to 400  BAD REQUEST
        checkWriteAccess(HttpStatus.SC_BAD_REQUEST, "picard", "picard", "sf", "public", 0);

        rh.sendAdminCertificate = true;
        response = rh.executePutRequest("/_security/role/security_role_starfleet_captains",
                FileHelper.loadFile("restapi/roles_complete_invalid.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

//		rh.sendAdminCertificate = true;
//		response = rh.executePutRequest("/_security/role/security_role_starfleet_captains",
//				FileHelper.loadFile("restapi/roles_multiple.json"), new Header[0]);
//		Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        response = rh.executePutRequest("/_security/role/security_role_starfleet_captains",
                FileHelper.loadFile("restapi/roles_multiple_2.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        // check tenants
        rh.sendAdminCertificate = true;
        response = rh.executePutRequest("/_security/role/security_role_starfleet_captains",
                FileHelper.loadFile("restapi/roles_captains_tenants.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = DefaultObjectMapper.readTree(response.getBody());
        Assert.assertEquals(2, settings.size());
        Assert.assertEquals(settings.get("status").asText(), "OK");


        response = rh.executeGetRequest("/_security/role/security_role_starfleet_captains", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        System.out.println(response.getBody());
        settings = DefaultObjectMapper.readTree(response.getBody());
        Assert.assertEquals(1, settings.size());
        Assert.assertEquals(new SecurityJsonNode(settings).getDotted("security_role_starfleet_captains.tenant_permissions").get(1).get("tenant_patterns").get(0).asString(), "tenant1");
        Assert.assertEquals(new SecurityJsonNode(settings).getDotted("security_role_starfleet_captains.tenant_permissions").get(1).get("privileges").get(0).asString(), "kibana_all_read");

        Assert.assertEquals(new SecurityJsonNode(settings).getDotted("security_role_starfleet_captains.tenant_permissions").get(0).get("tenant_patterns").get(0).asString(), "tenant2");
        Assert.assertEquals(new SecurityJsonNode(settings).getDotted("security_role_starfleet_captains.tenant_permissions").get(0).get("privileges").get(0).asString(), "kibana_all_write");


        response = rh.executePutRequest("/_security/role/security_role_starfleet_captains",
                FileHelper.loadFile("restapi/roles_captains_tenants2.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = DefaultObjectMapper.readTree(response.getBody());
        Assert.assertEquals(2, settings.size());
        Assert.assertEquals(settings.get("status").asText(), "OK");

        response = rh.executeGetRequest("/_security/role/security_role_starfleet_captains", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = DefaultObjectMapper.readTree(response.getBody());
        Assert.assertEquals(1, settings.size());

        Assert.assertEquals(new SecurityJsonNode(settings).getDotted("security_role_starfleet_captains.tenant_permissions").get(0).get("tenant_patterns").get(0).asString(), "tenant2");
        Assert.assertEquals(new SecurityJsonNode(settings).getDotted("security_role_starfleet_captains.tenant_permissions").get(0).get("tenant_patterns").get(1).asString(), "tenant4");

        Assert.assertEquals(new SecurityJsonNode(settings).getDotted("security_role_starfleet_captains.tenant_permissions").get(0).get("privileges").get(0).asString(), "kibana_all_write");

        Assert.assertEquals(new SecurityJsonNode(settings).getDotted("security_role_starfleet_captains.tenant_permissions").get(1).get("tenant_patterns").get(0).asString(), "tenant1");
        Assert.assertEquals(new SecurityJsonNode(settings).getDotted("security_role_starfleet_captains.tenant_permissions").get(1).get("tenant_patterns").get(1).asString(), "tenant3");
        Assert.assertEquals(new SecurityJsonNode(settings).getDotted("security_role_starfleet_captains.tenant_permissions").get(1).get("privileges").get(0).asString(), "kibana_all_read");

        // remove tenants from role
        response = rh.executePutRequest("/_security/role/security_role_starfleet_captains",
                FileHelper.loadFile("restapi/roles_captains_no_tenants.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = DefaultObjectMapper.readTree(response.getBody());
        Assert.assertEquals(2, settings.size());
        Assert.assertEquals(settings.get("status").asText(), "OK");

        response = rh.executeGetRequest("/_security/role/security_role_starfleet_captains", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = DefaultObjectMapper.readTree(response.getBody());
        Assert.assertEquals(1, settings.size());
        Assert.assertFalse(new SecurityJsonNode(settings).getDotted("security_role_starfleet_captains.cluster").get(0).isNull());
        Assert.assertTrue(new SecurityJsonNode(settings).getDotted("security_role_starfleet_captains.tenant_permissions").get(0).isNull());

        response = rh.executePutRequest("/_security/role/security_role_starfleet_captains",
                FileHelper.loadFile("restapi/roles_captains_tenants_malformed.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        settings = DefaultObjectMapper.readTree(response.getBody());
        Assert.assertEquals(settings.get("status").asText(), "error");
        Assert.assertEquals(settings.get("reason").asText(), AbstractConfigurationValidator.ErrorType.INVALID_CONFIGURATION.getMessage());

        // -- PATCH
        // PATCH on non-existing resource
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role/imnothere", "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // PATCH read only resource, must be forbidden
        // SuperAdmin can patch it
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role/security_transport_client", "[{ \"op\": \"add\", \"path\": \"/description\", \"value\": \"foo\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        // PATCH hidden resource, must be not found, can be found for superadmin, but will fail with no path present exception
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role/security_internal", "[{ \"op\": \"add\", \"path\": \"/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        // PATCH value of hidden flag, must fail with validation error
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role/security_role_starfleet", "[{ \"op\": \"add\", \"path\": \"/hidden\", \"value\": true }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody(), response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));

        List<String> permissions = null;

        // PATCH
        /*
         * how to patch with new v7 config format?
         * rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role/security_role_starfleet", "[{ \"op\": \"add\", \"path\": \"/indices/sf/ships/-\", \"value\": \"SEARCH\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_security/role/security_role_starfleet", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = DefaultObjectMapper.readTree(response.getBody());
        permissions = DefaultObjectMapper.objectMapper.convertValue(settings.get("security_role_starfleet").get("indices").get("sf").get("ships"), List.class);
        Assert.assertNotNull(permissions);
        Assert.assertEquals(2, permissions.size());
        Assert.assertTrue(permissions.contains("SECURITY_READ"));
        Assert.assertTrue(permissions.contains("SECURITY_SEARCH")); */

        // -- PATCH on whole config resource
        // PATCH on non-existing resource
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role", "[{ \"op\": \"add\", \"path\": \"/imnothere/a/b/c\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        // PATCH read only resource, must be forbidden
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role", "[{ \"op\": \"add\", \"path\": \"/security_transport_client/a\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        // PATCH hidden resource, must be bad request
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role", "[{ \"op\": \"add\", \"path\": \"/security_internal/a\", \"value\": [ \"foo\", \"bar\" ] }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

        // PATCH delete read only resource, must be forbidden
        // SuperAdmin can delete read only user
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role", "[{ \"op\": \"remove\", \"path\": \"/security_transport_client\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());

        // PATCH hidden resource, must be bad request, but allowed for superadmin
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role", "[{ \"op\": \"remove\", \"path\": \"/security_internal\"}]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(response.getBody().contains("\"message\":\"Resource updated."));

        // PATCH value of hidden flag, must fail with validation error
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role", "[{ \"op\": \"add\", \"path\": \"/newnewnew\", \"value\": {  \"hidden\": true, \"indices\" : [ {\"names\" : [ \"sf\" ],\"privileges\" : [ \"SECURITY_READ\" ]}] }}]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());
        Assert.assertTrue(response.getBody().matches(".*\"invalid_keys\"\\s*:\\s*\\{\\s*\"keys\"\\s*:\\s*\"hidden\"\\s*\\}.*"));

        // PATCH
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role", "[{ \"op\": \"add\", \"path\": \"/bulknew1\", \"value\": {   \"indices\" : [ {\"names\" : [ \"sf\" ],\"privileges\" : [ \"SECURITY_READ\" ]}] }}]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_security/role/bulknew1", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        settings = DefaultObjectMapper.readTree(response.getBody());
        permissions =  new SecurityJsonNode(settings).get("bulknew1").get("indices").get(0).get("privileges").asList();
        Assert.assertNotNull(permissions);
        Assert.assertEquals(1, permissions.size());
        Assert.assertTrue(permissions.contains("SECURITY_READ"));

        // delete resource
        rh.sendAdminCertificate = true;
        response = rh.executePatchRequest("/_security/role", "[{ \"op\": \"remove\", \"path\": \"/bulknew1\"}]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        response = rh.executeGetRequest("/_security/role/bulknew1", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // put valid field masks
        response = rh.executePutRequest("/_security/role/security_field_mask_valid",
                FileHelper.loadFile("restapi/roles_field_masks_valid.json"), new Header[0]);
        Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

        // put invalid field masks
        response = rh.executePutRequest("/_security/role/security_field_mask_invalid",
                FileHelper.loadFile("restapi/roles_field_masks_invalid.json"), new Header[0]);
        Assert.assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatusCode());

    }

    @Test
    public void testRolesApiForNonSuperAdmin() throws Exception {

        setupWithRestRoles();

        rh.keystore = "restapi/kirk-keystore.jks";
        rh.sendAdminCertificate = false;
        rh.sendHTTPClientCredentials = true;

        RestHelper.HttpResponse response;

        // Delete read only roles
        response = rh.executeDeleteRequest("/_security/role/security_transport_client" , new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        // Put read only roles
        response = rh.executePutRequest("/_security/role/security_transport_client", "[{ \"op\": \"replace\", \"path\": \"/description\", \"value\": \"foo\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        // Patch single read only roles
        response = rh.executePatchRequest("/_security/role/security_transport_client", "[{ \"op\": \"replace\", \"path\": \"/description\", \"value\": \"foo\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        // Patch multiple read only roles
        response = rh.executePatchRequest("/_security/role/", "[{ \"op\": \"add\", \"path\": \"/security_transport_client/description\", \"value\": \"foo\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatusCode());

        // get hidden role
        response = rh.executeGetRequest("_security/role/security_internal");
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // delete hidden role
        response = rh.executeDeleteRequest("/_security/role/security_internal" , new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // put hidden role
        response = rh.executePutRequest("/_security/role/security_internal", "[{ \"op\": \"replace\", \"path\": \"/description\", \"value\": \"foo\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // Patch single hidden roles
        response = rh.executePatchRequest("/_security/role/security_internal", "[{ \"op\": \"replace\", \"path\": \"/description\", \"value\": \"foo\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

        // Patch multiple hidden roles
        response = rh.executePatchRequest("/_security/role/", "[{ \"op\": \"add\", \"path\": \"/security_internal/description\", \"value\": \"foo\" }]", new Header[0]);
        Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusCode());

    }

}
