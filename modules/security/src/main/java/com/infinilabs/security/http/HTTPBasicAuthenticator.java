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

package com.infinilabs.security.http;

import com.infinilabs.security.auth.AuthenticationFailedExceptions;
import com.infinilabs.security.auth.HTTPAuthenticator;
import com.infinilabs.security.support.HTTPHelper;
import com.infinilabs.security.user.AuthCredentials;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.nio.file.Path;

//TODO FUTURE allow only if protocol==https
public class HTTPBasicAuthenticator implements HTTPAuthenticator {

    protected final Logger log = LogManager.getLogger(this.getClass());

    public HTTPBasicAuthenticator(final Settings settings, final Path configPath) {

    }

    @Override
    public AuthCredentials extractCredentials(final RestRequest request, ThreadContext threadContext) {

        final boolean forceLogin = request.paramAsBoolean("force_login", false);

        if(forceLogin) {
            return null;
        }

        final String authorizationHeader = request.header("Authorization");

        return HTTPHelper.extractCredentials(authorizationHeader, log);
    }

    @Override
    public boolean reRequestAuthentication(RestRequest request, final RestChannel channel, AuthCredentials creds) {
        ElasticsearchSecurityException e = AuthenticationFailedExceptions.createSecurityException("Missing authentication information for REST request [{}]", request.uri());
        try {
            channel.sendResponse(new BytesRestResponse(channel, RestStatus.UNAUTHORIZED, e) {

                @Override
                protected boolean skipStackTrace() {
                    return true;
                }
            });
        } catch (IOException ex) {
            ex.printStackTrace();
            log.error("failed to send failure response for uri [{}] reason [{}]", request.uri(), e.getMessage());
        }
        return true;
    }

    @Override
    public String getType() {
        return "basic";
    }
}
