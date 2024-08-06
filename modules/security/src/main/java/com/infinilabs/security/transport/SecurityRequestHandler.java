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

package com.infinilabs.security.transport;

import com.google.common.base.Strings;
import com.infinilabs.security.action.whoami.WhoAmIAction;
import com.infinilabs.security.auditlog.AuditLog;
import com.infinilabs.security.auditlog.AuditLog.Origin;
import com.infinilabs.security.auth.BackendRegistry;
import com.infinilabs.security.ssl.SslExceptionHandler;
import com.infinilabs.security.ssl.transport.PrincipalExtractor;
import com.infinilabs.security.ssl.transport.SecuritySSLRequestHandler;
import com.infinilabs.security.ssl.util.ExceptionUtils;
import com.infinilabs.security.ssl.util.SSLRequestHelper;
import com.infinilabs.security.support.Base64Helper;
import com.infinilabs.security.support.ConfigConstants;
import com.infinilabs.security.support.HeaderHelper;
import com.infinilabs.security.user.User;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ConcreteShardRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;

import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.infinilabs.security.SecurityPlugin.isActionTraceEnabled;

public class SecurityRequestHandler<T extends TransportRequest> extends SecuritySSLRequestHandler<T> {

    private final BackendRegistry backendRegistry;
    private final AuditLog auditLog;
    private final InterClusterRequestEvaluator requestEvalProvider;
    private final ClusterService cs;

    SecurityRequestHandler(String action,
                           final TransportRequestHandler<T> actualHandler,
                           final ThreadPool threadPool,
                           final BackendRegistry backendRegistry,
                           final AuditLog auditLog,
                           final PrincipalExtractor principalExtractor,
                           final InterClusterRequestEvaluator requestEvalProvider,
                           final ClusterService cs,
                           final SslExceptionHandler sslExceptionHandler) {
        super(action, actualHandler, threadPool, principalExtractor, sslExceptionHandler);
        this.backendRegistry = backendRegistry;
        this.auditLog = auditLog;
        this.requestEvalProvider = requestEvalProvider;
        this.cs = cs;
    }

    @Override
    protected void messageReceivedDecorate(final T request, final TransportRequestHandler<T> handler,
            final TransportChannel transportChannel, Task task) throws Exception {

        String resolvedActionClass = request.getClass().getSimpleName();

        if(request instanceof BulkShardRequest) {
            if(((BulkShardRequest) request).items().length == 1) {
                resolvedActionClass = ((BulkShardRequest) request).items()[0].request().getClass().getSimpleName();
            }
        }

        if(request instanceof ConcreteShardRequest) {
            resolvedActionClass = ((ConcreteShardRequest) request).getRequest().getClass().getSimpleName();
        }

        String initialActionClassValue = getThreadContext().getHeader(ConfigConstants.SECURITY_INITIAL_ACTION_CLASS_HEADER);

        final ThreadContext.StoredContext sgContext = getThreadContext().newStoredContext(false);

        final String originHeader = getThreadContext().getHeader(ConfigConstants.SECURITY_ORIGIN_HEADER);

        if(!Strings.isNullOrEmpty(originHeader)) {
            getThreadContext().putTransient(ConfigConstants.SECURITY_ORIGIN, originHeader);
        }

        try {

            if(transportChannel.getChannelType() == null) {
                throw new RuntimeException("Can not determine channel type (null)");
            }

            String channelType = transportChannel.getChannelType();

            if (!channelType.equals("direct") && !channelType.equals("transport")) {
                TransportChannel innerChannel = getInnerChannel(transportChannel);
                channelType = innerChannel.getChannelType();
            }

            getThreadContext().putTransient(ConfigConstants.SECURITY_CHANNEL_TYPE, channelType);
            getThreadContext().putTransient(ConfigConstants.SECURITY_ACTION_NAME, task.getAction());

            if(request instanceof ShardSearchRequest) {
                ShardSearchRequest sr = ((ShardSearchRequest) request);
                if(sr.source() != null && sr.source().suggest() != null) {
                    getThreadContext().putTransient("_security_issuggest", Boolean.TRUE);
                }
            }

            //bypass non-netty requests
            if(channelType.equals("direct")) {
                final String userHeader = getThreadContext().getHeader(ConfigConstants.SECURITY_USER_HEADER);
                final String injectedRolesHeader = getThreadContext().getHeader(ConfigConstants.SECURITY_INJECTED_ROLES_HEADER);
                final String injectedUserHeader = getThreadContext().getHeader(ConfigConstants.SECURITY_INJECTED_USER_HEADER);

                if(Strings.isNullOrEmpty(userHeader)) {
                    // Keeping role injection with higher priority as plugins under OpenSearch will be using this
                    // on transport layer
                    if(!Strings.isNullOrEmpty(injectedRolesHeader)) {
                        getThreadContext().putTransient(ConfigConstants.SECURITY_INJECTED_ROLES, injectedRolesHeader);
                    }
                    else if (!Strings.isNullOrEmpty(injectedUserHeader)) {
                        getThreadContext().putTransient(ConfigConstants.SECURITY_INJECTED_USER, injectedUserHeader);
                    }
                } else {
                    getThreadContext().putTransient(ConfigConstants.SECURITY_USER, Objects.requireNonNull((User) Base64Helper.deserializeObject(userHeader)));
                }

                final String originalRemoteAddress = getThreadContext().getHeader(ConfigConstants.SECURITY_REMOTE_ADDRESS_HEADER);

                if(!Strings.isNullOrEmpty(originalRemoteAddress)) {
                    getThreadContext().putTransient(ConfigConstants.SECURITY_REMOTE_ADDRESS, new TransportAddress((InetSocketAddress) Base64Helper.deserializeObject(originalRemoteAddress)));
                }

                final String rolesValidation = getThreadContext().getHeader(ConfigConstants.SECURITY_INJECTED_ROLES_VALIDATION_HEADER);
                if(!Strings.isNullOrEmpty(rolesValidation)) {
                    getThreadContext().putTransient(ConfigConstants.SECURITY_INJECTED_ROLES_VALIDATION, rolesValidation);
                }

                if (isActionTraceEnabled()) {
                    getThreadContext().putHeader("_security_trace"+System.currentTimeMillis()+"#"+UUID.randomUUID().toString(), Thread.currentThread().getName()+" DIR -> "+transportChannel.getChannelType()+" "+getThreadContext().getHeaders());
                }

                putInitialActionClassHeader(initialActionClassValue, resolvedActionClass);

                super.messageReceivedDecorate(request, handler, transportChannel, task);
                return;
            }

            //if the incoming request is an internal:* or a shard request allow only if request was sent by a server node
            //if transport channel is not a netty channel but a direct or local channel (e.g. send via network) then allow it (regardless of beeing a internal: or shard request)
            //also allow when issued from a remote cluster for cross cluster search
            if ( !HeaderHelper.isInterClusterRequest(getThreadContext())
                    && !HeaderHelper.isTrustedClusterRequest(getThreadContext())
                    && !task.getAction().equals("internal:transport/handshake")
                    && (task.getAction().startsWith("internal:") || task.getAction().contains("["))) {

                auditLog.logMissingPrivileges(task.getAction(), request, task);
                log.error("Internal or shard requests ("+task.getAction()+") not allowed from a non-server node for transport type "+transportChannel.getChannelType());
                transportChannel.sendResponse(new ElasticsearchSecurityException(
                            "Internal or shard requests not allowed from a non-server node for transport type "+transportChannel.getChannelType()));
                return;
                    }


            String principal = null;

            if ((principal = getThreadContext().getTransient(ConfigConstants.SECURITY_SSL_TRANSPORT_PRINCIPAL)) == null) {
                Exception ex = new ElasticsearchSecurityException(
                        "No SSL client certificates found for transport type "+transportChannel.getChannelType()+". Security needs the SSL plugin to be installed");
                auditLog.logSSLException(request, ex, task.getAction(), task);
                log.error("No SSL client certificates found for transport type "+transportChannel.getChannelType()+". Security needs the SSL plugin to be installed");
                transportChannel.sendResponse(ex);
                return;
            } else {

                if(getThreadContext().getTransient(ConfigConstants.SECURITY_ORIGIN) == null) {
                    getThreadContext().putTransient(ConfigConstants.SECURITY_ORIGIN, Origin.TRANSPORT.toString());
                }

                //network intercluster request or cross search cluster request
                if(HeaderHelper.isInterClusterRequest(getThreadContext())
                        || HeaderHelper.isTrustedClusterRequest(getThreadContext())) {

                    final String userHeader = getThreadContext().getHeader(ConfigConstants.SECURITY_USER_HEADER);
                    final String injectedRolesHeader = getThreadContext().getHeader(ConfigConstants.SECURITY_INJECTED_ROLES_HEADER);
                    final String injectedUserHeader = getThreadContext().getHeader(ConfigConstants.SECURITY_INJECTED_USER_HEADER);

                    if(Strings.isNullOrEmpty(userHeader)) {
                        // Keeping role injection with higher priority as plugins under OpenSearch will be using this
                        // on transport layer
                        if(!Strings.isNullOrEmpty(injectedRolesHeader)) {
                            getThreadContext().putTransient(ConfigConstants.SECURITY_INJECTED_ROLES, injectedRolesHeader);
                        }
                        else if(!Strings.isNullOrEmpty(injectedUserHeader)) {
                            getThreadContext().putTransient(ConfigConstants.SECURITY_INJECTED_USER, injectedUserHeader);
                        }
                    } else {
                        getThreadContext().putTransient(ConfigConstants.SECURITY_USER, Objects.requireNonNull((User) Base64Helper.deserializeObject(userHeader)));
                    }

                    String originalRemoteAddress = getThreadContext().getHeader(ConfigConstants.SECURITY_REMOTE_ADDRESS_HEADER);

                    if(!Strings.isNullOrEmpty(originalRemoteAddress)) {
                        getThreadContext().putTransient(ConfigConstants.SECURITY_REMOTE_ADDRESS, new TransportAddress((InetSocketAddress) Base64Helper.deserializeObject(originalRemoteAddress)));
                    } else {
                        getThreadContext().putTransient(ConfigConstants.SECURITY_REMOTE_ADDRESS, request.remoteAddress());
                    }

                    final String rolesValidation = getThreadContext().getHeader(ConfigConstants.SECURITY_INJECTED_ROLES_VALIDATION_HEADER);
                    if(!Strings.isNullOrEmpty(rolesValidation)) {
                        getThreadContext().putTransient(ConfigConstants.SECURITY_INJECTED_ROLES_VALIDATION, rolesValidation);
                    }

                } else {

                    //this is a netty request from a non-server node (maybe also be internal: or a shard request)
                    //and therefore issued by a transport client

                    if(SSLRequestHelper.containsBadHeader(getThreadContext(), ConfigConstants.SECURITY_CONFIG_PREFIX)) {
                        final ElasticsearchException exception = ExceptionUtils.createBadHeaderException();
                        auditLog.logBadHeaders(request, task.getAction(), task);
                        log.error(exception);
                        transportChannel.sendResponse(exception);
                        return;
                    }

                    //TODO INFINI Elasticsearch Security exception handling, introduce authexception

                    User user;
                    //try {
                    if((user = backendRegistry.authenticate(request, principal, task, task.getAction())) == null) {
                        org.apache.logging.log4j.ThreadContext.remove("user");

                        if(task.getAction().equals(WhoAmIAction.NAME)) {
                            super.messageReceivedDecorate(request, handler, transportChannel, task);
                            return;
                        }

                        if(task.getAction().equals("cluster:monitor/nodes/liveness")
                                || task.getAction().equals("internal:transport/handshake")) {
                            super.messageReceivedDecorate(request, handler, transportChannel, task);
                            return;
                                }


                        log.error("Cannot authenticate {} for {}", getThreadContext().getTransient(ConfigConstants.SECURITY_USER), task.getAction());
                        transportChannel.sendResponse(new ElasticsearchSecurityException("Cannot authenticate "+getThreadContext().getTransient(ConfigConstants.SECURITY_USER)));
                        return;
                    } else {
                        // make it possible to filter logs by username
                        org.apache.logging.log4j.ThreadContext.put("user", user.getName());
                    }
                    //} catch (Exception e) {
                    //    log.error("Error authentication transport user "+e, e);
                    //auditLog.logFailedLogin(principal, false, null, request);
                    //transportChannel.sendResponse(ExceptionsHelper.convertToElastic(e));
                    //return;
                    //}

                    getThreadContext().putTransient(ConfigConstants.SECURITY_USER, user);
                    TransportAddress originalRemoteAddress = request.remoteAddress();

                    if(originalRemoteAddress != null && (originalRemoteAddress instanceof TransportAddress)) {
                        getThreadContext().putTransient(ConfigConstants.SECURITY_REMOTE_ADDRESS, originalRemoteAddress);
                    } else {
                        log.error("Request has no proper remote address {}", originalRemoteAddress);
                        transportChannel.sendResponse(new ElasticsearchException("Request has no proper remote address"));
                        return;
                    }
                }

                if (isActionTraceEnabled()) {
                    getThreadContext().putHeader("_security_trace"+System.currentTimeMillis()+"#"+UUID.randomUUID().toString(), Thread.currentThread().getName()+" NETTI -> "+transportChannel.getChannelType()+" "+getThreadContext().getHeaders().entrySet().stream().filter(p->!p.getKey().startsWith("_security_trace")).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue())));
                }


                putInitialActionClassHeader(initialActionClassValue, resolvedActionClass);

                super.messageReceivedDecorate(request, handler, transportChannel, task);
            }
        } finally {

            if (isActionTraceEnabled()) {
                getThreadContext().putHeader("_security_trace"+System.currentTimeMillis()+"#"+UUID.randomUUID().toString(), Thread.currentThread().getName()+" FIN -> "+transportChannel.getChannelType()+" "+getThreadContext().getHeaders());
            }

            if(sgContext != null) {
                sgContext.close();
            }
        }
    }

    private void putInitialActionClassHeader(String initialActionClassValue, String resolvedActionClass) {
        if(initialActionClassValue == null) {
            if(getThreadContext().getHeader(ConfigConstants.SECURITY_INITIAL_ACTION_CLASS_HEADER) == null) {
                getThreadContext().putHeader(ConfigConstants.SECURITY_INITIAL_ACTION_CLASS_HEADER, resolvedActionClass);
            }
        } else {
            if(getThreadContext().getHeader(ConfigConstants.SECURITY_INITIAL_ACTION_CLASS_HEADER) == null) {
                getThreadContext().putHeader(ConfigConstants.SECURITY_INITIAL_ACTION_CLASS_HEADER, initialActionClassValue);
            }
        }

    }

    @Override
    protected void addAdditionalContextValues(final String action, final TransportRequest request, final X509Certificate[] localCerts, final X509Certificate[] peerCerts, final String principal)
            throws Exception {

        boolean isInterClusterRequest = requestEvalProvider.isInterClusterRequest(request, localCerts, peerCerts, principal);

        if (isInterClusterRequest) {
            if(cs.getClusterName().value().equals(getThreadContext().getHeader("_security_remotecn"))) {

                if (log.isTraceEnabled() && !action.startsWith("internal:")) {
                    log.trace("Is inter cluster request ({}/{}/{})", action, request.getClass(), request.remoteAddress());
                }

                getThreadContext().putTransient(ConfigConstants.SECURITY_SSL_TRANSPORT_INTERCLUSTER_REQUEST, Boolean.TRUE);
            } else {
                getThreadContext().putTransient(ConfigConstants.SECURITY_SSL_TRANSPORT_TRUSTED_CLUSTER_REQUEST, Boolean.TRUE);
            }

        } else {
            if (log.isTraceEnabled()) {
                log.trace("Is not an inter cluster request");
            }
        }

        super.addAdditionalContextValues(action, request, localCerts, peerCerts, principal);
    }
}
