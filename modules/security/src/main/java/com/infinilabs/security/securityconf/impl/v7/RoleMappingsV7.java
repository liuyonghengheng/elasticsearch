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
 * Copyright 2015-2017 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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

package com.infinilabs.security.securityconf.impl.v7;

import java.util.Collections;
import java.util.List;

import com.infinilabs.security.securityconf.Hideable;
import com.infinilabs.security.securityconf.RoleMappings;
import com.infinilabs.security.securityconf.impl.v6.RoleMappingsV6;

public class RoleMappingsV7 extends RoleMappings implements Hideable {

    private boolean reserved;
    private boolean hidden;

    public List<String> getExternal_roles() {
        return external_roles;
    }

    public void setExternal_roles(List<String> external_roles) {
        this.external_roles = external_roles;
    }

    private List<String> external_roles = Collections.emptyList();
    private List<String> and_external_roles= Collections.emptyList();
    private String description;

    public RoleMappingsV7() {
        super();
    }

    public List<String> getAnd_external_roles() {
        return and_external_roles;
    }

    public void setAnd_external_roles(List<String> and_external_roles) {
        this.and_external_roles = and_external_roles;
    }

    public RoleMappingsV7(RoleMappingsV6 roleMappingsV6) {
        super();
        this.reserved = roleMappingsV6.isReserved();
        this.hidden = roleMappingsV6.isHidden();
        this.external_roles = roleMappingsV6.getBackendroles();
        this.and_external_roles = roleMappingsV6.getAndBackendroles();
        this.description = "Migrated from v6";
        setHosts(roleMappingsV6.getHosts());
        setUsers(roleMappingsV6.getUsers());
    }

    public boolean isReserved() {
        return reserved;
    }



    public void setReserved(boolean reserved) {
        this.reserved = reserved;
    }


    public boolean isHidden() {
        return hidden;
    }


    public void setHidden(boolean hidden) {
        this.hidden = hidden;
    }


    public String getDescription() {
        return description;
    }


    public void setDescription(String description) {
        this.description = description;
    }



    @Override
    public String toString() {
        return "RoleMappings [reserved=" + reserved + ", hidden=" + hidden + ", external_roles=" + external_roles + ", hosts=" + getHosts() + ", users="
                + getUsers() + ", and_external_roles=" + and_external_roles + ", description=" + description + "]";
    }




}
