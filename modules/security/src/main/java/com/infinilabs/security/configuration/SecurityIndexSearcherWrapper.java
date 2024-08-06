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

package com.infinilabs.security.configuration;

import java.io.IOException;
import java.util.Set;

import com.infinilabs.security.privileges.PrivilegesEvaluator;
import com.infinilabs.security.securityconf.ConfigModel;
import com.infinilabs.security.support.WildcardMatcher;
import com.infinilabs.security.support.ConfigConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.greenrobot.eventbus.Subscribe;

import com.infinilabs.security.support.HeaderHelper;
import com.infinilabs.security.user.User;

public class SecurityIndexSearcherWrapper implements CheckedFunction<DirectoryReader, DirectoryReader, IOException>  {

    protected final Logger log = LogManager.getLogger(this.getClass());
    protected final ThreadContext threadContext;
    protected final Index index;
    protected final String opendistrosecurityIndex;
    private final AdminDNs adminDns;
    private ConfigModel configModel;
    private final PrivilegesEvaluator evaluator;

    private final Boolean systemIndexEnabled;
    private final WildcardMatcher systemIndexMatcher;

    //constructor is called per index, so avoid costly operations here
    public SecurityIndexSearcherWrapper(final IndexService indexService, final Settings settings, final AdminDNs adminDNs, final PrivilegesEvaluator evaluator) {
        index = indexService.index();
        threadContext = indexService.getThreadPool().getThreadContext();
        this.opendistrosecurityIndex = settings.get(ConfigConstants.SECURITY_CONFIG_INDEX_NAME, ConfigConstants.SECURITY_DEFAULT_CONFIG_INDEX);
        this.evaluator = evaluator;
        this.adminDns = adminDNs;
        this.systemIndexEnabled = settings.getAsBoolean(ConfigConstants.SECURITY_SYSTEM_INDICES_ENABLED_KEY, ConfigConstants.SECURITY_SYSTEM_INDICES_ENABLED_DEFAULT);
        this.systemIndexMatcher = WildcardMatcher.from(settings.getAsList(ConfigConstants.SECURITY_SYSTEM_INDICES_KEY));
    }

    @Subscribe
    public void onConfigModelChanged(ConfigModel cm) {
        this.configModel = cm;
    }

    @Override
    public final DirectoryReader apply(DirectoryReader reader) throws IOException {

        if (isSecurityIndexRequest() && !isAdminAuthenticatedOrInternalRequest()) {
            return new EmptyFilterLeafReader.EmptyDirectoryReader(reader);
        }

        if (systemIndexEnabled && isBlockedSystemIndexRequest() && !isAdminDnOrPluginRequest()) {
            log.warn("search action for {} is not allowed for a non adminDN user", index.getName());
            return new EmptyFilterLeafReader.EmptyDirectoryReader(reader);
        }

        return dlsFlsWrap(reader, isAdminAuthenticatedOrInternalRequest());
    }

    protected DirectoryReader dlsFlsWrap(final DirectoryReader reader, boolean isAdmin) throws IOException {
        return reader;
    }

    protected final boolean isAdminAuthenticatedOrInternalRequest() {

        final User user = (User) threadContext.getTransient(ConfigConstants.SECURITY_USER);

        if (user != null && adminDns.isAdmin(user)) {
            return true;
        }

        if ("true".equals(HeaderHelper.getSafeFromHeader(threadContext, ConfigConstants.SECURITY_CONF_REQUEST_HEADER))) {
            return true;
        }

        return false;
    }

    protected final boolean isSecurityIndexRequest() {
        return index.getName().equals(opendistrosecurityIndex);
    }

    protected final boolean isBlockedSystemIndexRequest() {
        return systemIndexMatcher.test(index.getName());
    }

    protected final boolean isAdminDnOrPluginRequest() {
        final User user = threadContext.getTransient(ConfigConstants.SECURITY_USER);
        if(user == null) {
            // allow request without user from plugin.
            return true;
        } else if (adminDns.isAdmin(user)) {
            return true;
        } else {
            return false;
        }
    }
}
