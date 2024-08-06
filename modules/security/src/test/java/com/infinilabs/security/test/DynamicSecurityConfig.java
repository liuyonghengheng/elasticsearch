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
 * Copyright 2015-2018 _floragunn_ GmbH
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Portions Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.infinilabs.security.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.infinilabs.security.securityconf.impl.CType;
import com.infinilabs.security.test.helper.file.FileHelper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;

public class DynamicSecurityConfig {

    private String securityIndexName = ".security";
    private String securityConfig = "config.yml";
    private String securityRoles = "roles.yml";
    private String securityRolesMapping = "roles_mapping.yml";
    private String securityInternalUsers = "users.yml";
    private String securityActionGroups = "privilege.yml";
    private String securityNodesDn = "nodes_dn.yml";
    private String securityWhitelist= "whitelist.yml";
    private String securityAudit = "audit.yml";
    private String securityConfigAsYamlString = null;
    private String type = "_doc";
    private String legacyConfigFolder = "";

    public String getSecurityIndexName() {
        return securityIndexName;
    }

    public DynamicSecurityConfig setSecurityIndexName(String securityIndexName) {
        this.securityIndexName = securityIndexName;
        return this;
    }

    public DynamicSecurityConfig setConfig(String securityConfig) {
        this.securityConfig = securityConfig;
        return this;
    }

    public DynamicSecurityConfig setConfigAsYamlString(String securityConfigAsYamlString) {
        this.securityConfigAsYamlString = securityConfigAsYamlString;
        return this;
    }

    public DynamicSecurityConfig setSecurityRoles(String securityRoles) {
        this.securityRoles = securityRoles;
        return this;
    }

    public DynamicSecurityConfig setSecurityRolesMapping(String securityRolesMapping) {
        this.securityRolesMapping = securityRolesMapping;
        return this;
    }

    public DynamicSecurityConfig setSecurityInternalUsers(String securityInternalUsers) {
        this.securityInternalUsers = securityInternalUsers;
        return this;
    }

    public DynamicSecurityConfig setSecurityActionGroups(String securityActionGroups) {
        this.securityActionGroups = securityActionGroups;
        return this;
    }

    public DynamicSecurityConfig setSecurityNodesDn(String nodesDn) {
        this.securityNodesDn = nodesDn;
        return this;
    }

    public DynamicSecurityConfig setSecurityWhitelist(String whitelist){
        this.securityWhitelist = whitelist;
        return this;
    }

    public DynamicSecurityConfig setSecurityAudit(String audit) {
        this.securityAudit = audit;
        return this;
    }

    public DynamicSecurityConfig setLegacy() {
        this.type = "security";
        this.legacyConfigFolder = "legacy/securityconfig_v6/";
        return this;
    }
    public String getType() {
        return type;
    }

    public List<IndexRequest> getDynamicConfig(String folder) {

        final String prefix = legacyConfigFolder+(folder == null?"":folder+"/");

        List<IndexRequest> ret = new ArrayList<IndexRequest>();

        ret.add(new IndexRequest(securityIndexName)
                .type(type)
                .id(CType.CONFIG.toLCString())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source(CType.CONFIG.toLCString(), securityConfigAsYamlString==null? FileHelper.readYamlContent(prefix+securityConfig):FileHelper.readYamlContentFromString(securityConfigAsYamlString)));

        ret.add(new IndexRequest(securityIndexName)
                .type(type)
                .id(CType.PRIVILEGE.toLCString())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source(CType.PRIVILEGE.toLCString(), FileHelper.readYamlContent(prefix+securityActionGroups)));

        ret.add(new IndexRequest(securityIndexName)
                .type(type)
                .id(CType.USER.toLCString())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source(CType.USER.toLCString(), FileHelper.readYamlContent(prefix+securityInternalUsers)));

        ret.add(new IndexRequest(securityIndexName)
                .type(type)
                .id(CType.ROLE.toLCString())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source(CType.ROLE.toLCString(), FileHelper.readYamlContent(prefix+securityRoles)));

        ret.add(new IndexRequest(securityIndexName)
                .type(type)
                .id(CType.ROLE_MAPPING.toLCString())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source(CType.ROLE_MAPPING.toLCString(), FileHelper.readYamlContent(prefix+securityRolesMapping)));

        if (null != FileHelper.getAbsoluteFilePathFromClassPath(prefix + securityNodesDn)) {
            ret.add(new IndexRequest(securityIndexName)
                    .type(type)
                    .id(CType.NODESDN.toLCString())
                    .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source(CType.NODESDN.toLCString(), FileHelper.readYamlContent(prefix + securityNodesDn)));

        }

        final String whitelistYmlFile = prefix + securityWhitelist;
        if (null != FileHelper.getAbsoluteFilePathFromClassPath(whitelistYmlFile)) {
            ret.add(new IndexRequest(securityIndexName)
                    .type(type)
                    .id(CType.WHITELIST.toLCString())
                    .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source(CType.WHITELIST.toLCString(), FileHelper.readYamlContent(whitelistYmlFile)));
        }

        final String auditYmlFile = prefix + securityAudit;
        if (null != FileHelper.getAbsoluteFilePathFromClassPath(auditYmlFile)) {
            ret.add(new IndexRequest(securityIndexName)
                    .type(type)
                    .id(CType.AUDIT.toLCString())
                    .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source(CType.AUDIT.toLCString(), FileHelper.readYamlContent(auditYmlFile)));
        }

        return Collections.unmodifiableList(ret);
    }

}
