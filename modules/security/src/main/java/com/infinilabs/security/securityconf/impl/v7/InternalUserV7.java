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
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.infinilabs.security.securityconf.Hashed;
import com.infinilabs.security.securityconf.Hideable;
import com.infinilabs.security.securityconf.StaticDefinable;
import com.infinilabs.security.securityconf.impl.v6.InternalUserV6;

public class InternalUserV7 implements Hideable, Hashed, StaticDefinable {

        private String hash;
        private boolean reserved;
        private boolean hidden;
        @JsonProperty(value = "static")
        private boolean _static;
        private List<String> external_roles = Collections.emptyList();
        private Map<String, String> attributes = Collections.emptyMap();
        private String description;

    public List<String> getRoles() {
        return roles;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    private List<String> roles = Collections.emptyList();

        private InternalUserV7(String hash, boolean reserved, boolean hidden, List<String> external_roles, Map<String, String> attributes) {
            super();
            this.hash = hash;
            this.reserved = reserved;
            this.hidden = hidden;
            this.external_roles = external_roles;
            this.attributes = attributes;
        }

        public InternalUserV7() {
            super();
            //default constructor
        }

        public InternalUserV7(InternalUserV6 u6) {
            hash = u6.getHash();
            reserved = u6.isReserved();
            hidden = u6.isHidden();
            external_roles = u6.getRoles();
            attributes = u6.getAttributes();
            description = "Migrated from v6";
        }

        public String getHash() {
            return hash;
        }
        public void setHash(String hash) {
            this.hash = hash;
        }



        public boolean isHidden() {
            return hidden;
        }
        public void setHidden(boolean hidden) {
            this.hidden = hidden;
        }


        public List<String> getExternal_roles() {
            return external_roles;
        }

        public void setExternal_roles(List<String> external_roles) {
            this.external_roles = external_roles;
        }

        public Map<String, String> getAttributes() {
            return attributes;
        }
        public void setAttributes(Map<String, String> attributes) {
            this.attributes = attributes;
        }

        @Override
        public String toString() {
            return "InternalUserV7 [hash=" + hash + ", reserved=" + reserved + ", hidden=" + hidden + ", _static=" + _static + ", external_roles="
                    + external_roles + ", attributes=" + attributes + ", description=" + description + "]";
        }

        @Override
        @JsonIgnore
        public void clearHash() {
            hash = "";
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
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


    }
