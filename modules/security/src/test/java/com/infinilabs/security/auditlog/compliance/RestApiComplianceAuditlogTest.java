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

package com.infinilabs.security.auditlog.compliance;

import com.infinilabs.security.auditlog.integration.TestAuditlogImpl;
import com.infinilabs.security.support.ConfigConstants;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.infinilabs.security.auditlog.AbstractAuditlogiUnitTest;
import com.infinilabs.security.test.helper.rest.RestHelper.HttpResponse;

public class RestApiComplianceAuditlogTest extends AbstractAuditlogiUnitTest {

    @Test
    public void testRestApiRolesEnabled() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("security.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SECURITY_RESTAPI_ROLES_ENABLED, "security_superuser")
                .put(ConfigConstants.SECURITY_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SECURITY_AUDIT_ENABLE_REST, true)
                .put(ConfigConstants.SECURITY_AUDIT_RESOLVE_BULK_REQUESTS, false)
                .put(ConfigConstants.SECURITY_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SECURITY_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, true)
                .put(ConfigConstants.SECURITY_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put(ConfigConstants.SECURITY_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .build();

        setup(additionalSettings);
        TestAuditlogImpl.clear();
        String body = "{ \"password\":\"test\",\"external_roles\":[\"role1\",\"role2\"] }";
        HttpResponse response = rh.executePutRequest("_security/user/compuser?pretty", body, encodeBasicHeader("admin", "admin"));
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
        Assert.assertTrue(TestAuditlogImpl.messages.size()+"",TestAuditlogImpl.messages.size() == 1);
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_request_effective_user"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("UPDATE"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }

    @Test
    public void testRestApiRolesDisabled() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("security.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SECURITY_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SECURITY_AUDIT_ENABLE_REST, true)
                .put(ConfigConstants.SECURITY_AUDIT_RESOLVE_BULK_REQUESTS, false)
                .put(ConfigConstants.SECURITY_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SECURITY_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, true)
                .put(ConfigConstants.SECURITY_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put(ConfigConstants.SECURITY_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .build();

        setup(additionalSettings);
        TestAuditlogImpl.clear();
        String body = "{ \"password\":\"test\",\"external_roles\":[\"role1\",\"role2\"] }";

        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendAdminCertificate = true;
        rh.keystore = "kirk-keystore.jks";

        HttpResponse response = rh.executePutRequest("_security/user/compuser?pretty", body);
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
        Assert.assertTrue(TestAuditlogImpl.messages.size()+"",TestAuditlogImpl.messages.size() == 1);
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_request_effective_user"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("UPDATE"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }

    @Test
    @Ignore
    public void testRestApiRolesDisabledGet() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("security.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SECURITY_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SECURITY_AUDIT_ENABLE_REST, true)
                .put(ConfigConstants.SECURITY_AUDIT_RESOLVE_BULK_REQUESTS, false)
                .put(ConfigConstants.SECURITY_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SECURITY_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, true)
                .put(ConfigConstants.SECURITY_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put(ConfigConstants.SECURITY_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .build();

        setup(additionalSettings);
        TestAuditlogImpl.clear();

        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendAdminCertificate = true;
        rh.keystore = "kirk-keystore.jks";
        System.out.println("----rest");
        HttpResponse response = rh.executeGetRequest("_security/role_mapping/security_superuser?pretty");
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(TestAuditlogImpl.messages.size() > 2);
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_request_effective_user"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("UPDATE"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }



    @Test
    public void testAutoInit() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("security.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SECURITY_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SECURITY_AUDIT_ENABLE_REST, true)
                .put(ConfigConstants.SECURITY_AUDIT_RESOLVE_BULK_REQUESTS, false)
                .put(ConfigConstants.SECURITY_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, true)
                .put(ConfigConstants.SECURITY_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, true)
                .put(ConfigConstants.SECURITY_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put(ConfigConstants.SECURITY_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .build();

        setup(additionalSettings);

        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());

        Assert.assertTrue(TestAuditlogImpl.messages.size() > 2);
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_request_effective_user"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_EXTERNAL_CONFIG"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }

    @Test
    public void testRestApiNewUser() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("security.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SECURITY_RESTAPI_ROLES_ENABLED, "security_superuser")
                .put(ConfigConstants.SECURITY_AUDIT_ENABLE_TRANSPORT, false)
                .put(ConfigConstants.SECURITY_AUDIT_ENABLE_REST, false)
                .put(ConfigConstants.SECURITY_AUDIT_RESOLVE_BULK_REQUESTS, false)
                .put(ConfigConstants.SECURITY_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SECURITY_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, true)
                .put(ConfigConstants.SECURITY_COMPLIANCE_HISTORY_WRITE_IGNORE_USERS, "admin")
                .build();

        setup(additionalSettings);
        TestAuditlogImpl.clear();
        String body = "{ \"password\":\"test\",\"external_roles\":[\"role1\",\"role2\"] }";
        System.out.println("exec");
        HttpResponse response = rh.executePutRequest("_security/user/compuser?pretty", body, encodeBasicHeader("admin", "admin"));
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(HttpStatus.SC_CREATED, response.getStatusCode());
        Assert.assertTrue(TestAuditlogImpl.messages.size()+"", TestAuditlogImpl.messages.isEmpty());
    }

    @Test
    public void testRestInternalConfigRead() throws Exception {

        Settings additionalSettings = Settings.builder()
                .put("security.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SECURITY_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SECURITY_RESTAPI_ROLES_ENABLED, "security_superuser")
                .put(ConfigConstants.SECURITY_AUDIT_ENABLE_REST, true)
                .put(ConfigConstants.SECURITY_AUDIT_RESOLVE_BULK_REQUESTS, false)
                .put(ConfigConstants.SECURITY_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SECURITY_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED, true)
                .put(ConfigConstants.SECURITY_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put(ConfigConstants.SECURITY_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .build();

        setup(additionalSettings);
        TestAuditlogImpl.clear();

        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendAdminCertificate = true;
        rh.keystore = "kirk-keystore.jks";
        System.out.println("req");
        HttpResponse response = rh.executeGetRequest("_security/user/admin?pretty");
        Thread.sleep(1500);
        System.out.println(TestAuditlogImpl.sb.toString());
        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());
        Assert.assertTrue(TestAuditlogImpl.messages.size()+"",TestAuditlogImpl.messages.size() == 1);
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("audit_request_effective_user"));
        Assert.assertTrue(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_READ"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("COMPLIANCE_INTERNAL_CONFIG_WRITE"));
        Assert.assertFalse(TestAuditlogImpl.sb.toString().contains("UPDATE"));
        Assert.assertTrue(validateMsgs(TestAuditlogImpl.messages));
    }

}
