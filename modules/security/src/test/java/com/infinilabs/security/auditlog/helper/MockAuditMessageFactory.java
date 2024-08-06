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

package com.infinilabs.security.auditlog.helper;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;

import com.infinilabs.security.auditlog.AuditLog;
import com.infinilabs.security.auditlog.impl.AuditCategory;
import com.infinilabs.security.auditlog.impl.AuditMessage;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;

public class MockAuditMessageFactory {

	public static AuditMessage validAuditMessage() {
		return validAuditMessage(AuditCategory.FAILED_LOGIN);
	}

	public static AuditMessage validAuditMessage(AuditCategory category) {

	    ClusterService cs = mock(ClusterService.class);
	    DiscoveryNode dn = mock(DiscoveryNode.class);

        when(dn.getHostAddress()).thenReturn("hostaddress");
        when(dn.getId()).thenReturn("hostaddress");
        when(dn.getHostName()).thenReturn("hostaddress");
        when(cs.localNode()).thenReturn(dn);
        when(cs.getClusterName()).thenReturn(new ClusterName("testcluster"));

		TransportAddress ta = new TransportAddress(new InetSocketAddress("8.8.8.8",80));

		AuditMessage msg = new AuditMessage(category, cs, AuditLog.Origin.TRANSPORT, AuditLog.Origin.TRANSPORT);
		msg.addEffectiveUser("John Doe");
		msg.addRemoteAddress(ta);
		msg.addRequestType("IndexRequest");
		return msg;
	}

}
