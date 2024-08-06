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
 * Copyright 2015-2019 floragunn GmbH
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

package com.infinilabs.security.auth.limiting;

import java.net.InetAddress;
import java.nio.file.Path;

import com.infinilabs.security.auth.AuthFailureListener;
import com.infinilabs.security.user.AuthCredentials;
import org.elasticsearch.common.settings.Settings;

import com.infinilabs.security.auth.blocking.ClientBlockRegistry;

public class UserNameBasedRateLimiter extends AbstractRateLimiter<String> implements AuthFailureListener, ClientBlockRegistry<String> {

    public UserNameBasedRateLimiter(Settings settings, Path configPath) {
        super(settings, configPath, String.class);
    }

    @Override
    public void onAuthFailure(InetAddress remoteAddress, AuthCredentials authCredentials, Object request) {
        if (authCredentials != null && authCredentials.getUsername() != null && this.rateTracker.track(authCredentials.getUsername())) {
            block(authCredentials.getUsername());
        }
    }
}
