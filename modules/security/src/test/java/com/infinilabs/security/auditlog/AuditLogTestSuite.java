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

package com.infinilabs.security.auditlog;

import com.infinilabs.security.auditlog.compliance.ComplianceAuditlogTest;
import com.infinilabs.security.auditlog.compliance.RestApiComplianceAuditlogTest;
import com.infinilabs.security.auditlog.integration.BasicAuditlogTest;
import com.infinilabs.security.auditlog.integration.SSLAuditlogTest;
import com.infinilabs.security.auditlog.routing.FallbackTest;
import com.infinilabs.security.auditlog.sink.SinkProviderTLSTest;
import com.infinilabs.security.auditlog.sink.WebhookAuditLogTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.infinilabs.security.auditlog.impl.AuditlogTest;
import com.infinilabs.security.auditlog.impl.DisabledCategoriesTest;
import com.infinilabs.security.auditlog.impl.IgnoreAuditUsersTest;
import com.infinilabs.security.auditlog.impl.TracingTests;

@RunWith(Suite.class)

@Suite.SuiteClasses({
	ComplianceAuditlogTest.class,
	RestApiComplianceAuditlogTest.class,
	AuditlogTest.class,
	//DelegateTest.class,
	DisabledCategoriesTest.class,
	IgnoreAuditUsersTest.class,
	TracingTests.class,
	BasicAuditlogTest.class,
	SSLAuditlogTest.class,
	FallbackTest.class,
//	RouterTest.class,
//	RoutingConfigurationTest.class,
//	SinkProviderTest.class,
	SinkProviderTLSTest.class,
	WebhookAuditLogTest.class,
})
public class AuditLogTestSuite {

}
