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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.infinilabs.security.securityconf.Hideable;
import com.infinilabs.security.securityconf.StaticDefinable;
import com.infinilabs.security.securityconf.impl.v6.RoleV6;

public class RoleV7 implements Hideable, StaticDefinable {

    private boolean reserved;
    private boolean hidden;
    @JsonProperty(value = "static")
    private boolean _static;
    private String description;
    private List<String> cluster = Collections.emptyList();
    private List<Index> indices = Collections.emptyList();

    public RoleV7() {

    }

    public RoleV7(RoleV6 roleV6) {
        this.reserved = roleV6.isReserved();
        this.hidden = roleV6.isHidden();
        this.description = "Migrated from v6 (all types mapped)";
        this.cluster = roleV6.getCluster();
        indices = new ArrayList<>();

        for(Entry<String, RoleV6.Index> v6i: roleV6.getIndices().entrySet()) {
            indices.add(new Index(v6i.getKey(), v6i.getValue()));
        }

    }

    public static class Index {

        private List<String> names = Collections.emptyList();
        private String query;

        public List<String> getField_security() {
            return field_security;
        }

        public void setField_security(List<String> field_security) {
            this.field_security = field_security;
        }

        private List<String> field_security = Collections.emptyList();
        private List<String> field_mask = Collections.emptyList();
        private List<String> privileges = Collections.emptyList();

        public Index(String pattern, RoleV6.Index v6Index) {
            super();
            names = Collections.singletonList(pattern);
            query = v6Index.get_dls_();
            field_security = v6Index.get_fls_();
            field_mask = v6Index.get_masked_fields_();
            Set<String> tmpActions = new HashSet<>();
            for(Entry<String, List<String>> type: v6Index.getTypes().entrySet()) {
                tmpActions.addAll(type.getValue());
            }
            privileges = new ArrayList<>(tmpActions);
        }


        public Index() {
            super();
        }

        public List<String> getNames() {
            return names;
        }
        public void setNames(List<String> names) {
            this.names = names;
        }
        public String getQuery() {
            return query;
        }
        public void setDls(String query) {
            this.query = query;
        }
        public List<String> getField_mask() {
            return field_mask;
        }
        public void setField_mask(List<String> field_mask) {
            this.field_mask = field_mask;
        }
        public List<String> getPrivileges() {
            return privileges;
        }
        public void setPrivileges(List<String> privileges) {
            this.privileges = privileges;
        }
        @Override
        public String toString() {
            return "Index [names=" + names + ", query=" + query + ", field_security=" + field_security + ", field_mask=" + field_mask
                    + ", privileges=" + privileges + "]";
        }
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

    public List<String> getCluster() {
        return cluster;
    }

    public void setCluster(List<String> cluster) {
        this.cluster = cluster;
    }



    public List<Index> getIndices() {
        return indices;
    }

    public void setIndices(List<Index> indices) {
        this.indices = indices;
    }

    public boolean isReserved() {
        return reserved;
    }

    public void setReserved(boolean reserved) {
        this.reserved = reserved;
    }

    @JsonProperty(value = "static")
    public boolean isStatic() {
        return _static;
    }
    @JsonProperty(value = "static")
    public void setStatic(boolean _static) {
        this._static = _static;
    }

    @Override
    public String toString() {
        return "RoleV7 [reserved=" + reserved + ", hidden=" + hidden + ", _static=" + _static + ", description=" + description
                + ", cluster=" + cluster + ", indices=" + indices + "]";
    }





}
