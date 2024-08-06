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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.infinilabs.security.auditlog.config;

import com.infinilabs.security.support.ConfigConstants;
import org.elasticsearch.common.settings.Settings;

public class ThreadPoolConfig {
    private static final int DEFAULT_THREAD_POOL_SIZE = 10;
    private static final int DEFAULT_THREAD_POOL_MAX_QUEUE_LEN = 100_000;

    private final int threadPoolSize;
    private final int threadPoolMaxQueueLen;

    public ThreadPoolConfig(int threadPoolSize, int threadPoolMaxQueueLen) {
        if (threadPoolSize <= 0) {
            throw new IllegalArgumentException("Incorrect thread pool size: " + threadPoolSize + " configured for audit logging.");
        }

        if (threadPoolMaxQueueLen <= 0) {
            throw new IllegalArgumentException("Incorrect thread pool queue length: " + threadPoolMaxQueueLen + " configured for audit logging.");
        }

        this.threadPoolSize = threadPoolSize;
        this.threadPoolMaxQueueLen = threadPoolMaxQueueLen;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public int getThreadPoolMaxQueueLen() {
        return threadPoolMaxQueueLen;
    }

    public static ThreadPoolConfig getConfig(Settings settings) {
        int threadPoolSize = settings.getAsInt(ConfigConstants.SECURITY_AUDIT_THREADPOOL_SIZE, DEFAULT_THREAD_POOL_SIZE);
        int threadPoolMaxQueueLen = settings.getAsInt(ConfigConstants.SECURITY_AUDIT_THREADPOOL_MAX_QUEUE_LEN, DEFAULT_THREAD_POOL_MAX_QUEUE_LEN);

        return new ThreadPoolConfig(threadPoolSize, threadPoolMaxQueueLen);
    }
}
