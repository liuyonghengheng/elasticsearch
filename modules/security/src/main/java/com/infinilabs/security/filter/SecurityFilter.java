

package com.infinilabs.security.filter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.infinilabs.security.action.whoami.WhoAmIAction;
import com.infinilabs.security.auditlog.AuditLog;
import com.infinilabs.security.auth.BackendRegistry;
import com.infinilabs.security.auth.RolesInjector;
import com.infinilabs.security.compliance.ComplianceConfig;
import com.infinilabs.security.configuration.AdminDNs;
import com.infinilabs.security.configuration.CompatConfig;
import com.infinilabs.security.configuration.DlsFlsRequestValve;
import com.infinilabs.security.privileges.PrivilegesEvaluator;
import com.infinilabs.security.privileges.PrivilegesEvaluatorResponse;
import com.infinilabs.security.resolver.IndexResolverReplacer;
import com.infinilabs.security.support.*;
import com.infinilabs.security.user.User;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.infinilabs.security.SecurityPlugin.isActionTraceEnabled;
import static com.infinilabs.security.SecurityPlugin.traceAction;

public class SecurityFilter implements ActionFilter {

    protected final Logger log = LogManager.getLogger(this.getClass());
    private final PrivilegesEvaluator evalp;
    private final AdminDNs adminDns;
    private DlsFlsRequestValve dlsFlsValve;
    private final AuditLog auditLog;
    private final ThreadContext threadContext;
    private final ClusterService cs;
    private final CompatConfig compatConfig;
    private final IndexResolverReplacer indexResolverReplacer;
    private final WildcardMatcher immutableIndicesMatcher;
    private final RolesInjector rolesInjector;
    private final Client client;
    private final BackendRegistry backendRegistry;

    public SecurityFilter(final Client client, final Settings settings, final PrivilegesEvaluator evalp, final AdminDNs adminDns,
                          DlsFlsRequestValve dlsFlsValve, AuditLog auditLog, ThreadPool threadPool, ClusterService cs,
                          final CompatConfig compatConfig, final IndexResolverReplacer indexResolverReplacer, BackendRegistry backendRegistry) {
        this.client = client;
        this.evalp = evalp;
        this.adminDns = adminDns;
        this.dlsFlsValve = dlsFlsValve;
        this.auditLog = auditLog;
        this.threadContext = threadPool.getThreadContext();
        this.cs = cs;
        this.compatConfig = compatConfig;
        this.indexResolverReplacer = indexResolverReplacer;
        this.immutableIndicesMatcher = WildcardMatcher.from(settings.getAsList(ConfigConstants.SECURITY_COMPLIANCE_IMMUTABLE_INDICES, Collections.emptyList()));
        this.rolesInjector = new RolesInjector();
        this.backendRegistry = backendRegistry;
        log.info("{} indices are made immutable.", immutableIndicesMatcher);
    }

    @VisibleForTesting
    WildcardMatcher getImmutableIndicesMatcher() {
        return immutableIndicesMatcher;
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, final String action, Request request,
            ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
        try (StoredContext ctx = threadContext.newStoredContext(true)){
            org.apache.logging.log4j.ThreadContext.clearAll();
            apply0(task, action, request, listener, chain);
        }
    }

    private static Set<String> alias2Name(Set<Alias> aliases) {
        return aliases.stream().map(a -> a.name()).collect(ImmutableSet.toImmutableSet());
    }

    private <Request extends ActionRequest, Response extends ActionResponse> void apply0(Task task, final String action, Request request,
            ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
        try {

            if(threadContext.getTransient(ConfigConstants.SECURITY_ORIGIN) == null) {
                threadContext.putTransient(ConfigConstants.SECURITY_ORIGIN, AuditLog.Origin.LOCAL.toString());
            }

            final ComplianceConfig complianceConfig = auditLog.getComplianceConfig();
            if (complianceConfig != null && complianceConfig.isEnabled()) {
                attachSourceFieldContext(request);
            }
            final Set<String> injectedRoles = rolesInjector.injectUserAndRoles(threadContext);
            boolean enforcePrivilegesEvaluation = false;
            User user = threadContext.getTransient(ConfigConstants.SECURITY_USER);
            if(user == null && (user = backendRegistry.authenticate(request, null, task, action)) != null) {
                threadContext.putTransient(ConfigConstants.SECURITY_USER, user);
                enforcePrivilegesEvaluation = true;
            }

            final boolean userIsAdmin = isUserAdmin(user, adminDns);
            final boolean interClusterRequest = HeaderHelper.isInterClusterRequest(threadContext);
            final boolean trustedClusterRequest = HeaderHelper.isTrustedClusterRequest(threadContext);
            final boolean confRequest = "true".equals(HeaderHelper.getSafeFromHeader(threadContext, ConfigConstants.SECURITY_CONF_REQUEST_HEADER));
            final boolean passThroughRequest = action.startsWith("indices:admin/seq_no")
                    || action.equals(WhoAmIAction.NAME);

            final boolean internalRequest =
                    (interClusterRequest || HeaderHelper.isDirectRequest(threadContext))
                    && action.startsWith("internal:")
                    && !action.startsWith("internal:transport/proxy");

            if (user != null) {
                org.apache.logging.log4j.ThreadContext.put("user", user.getName());
            }

            if (isActionTraceEnabled()) {

                String count = "";
                if(request instanceof BulkRequest) {
                    count = ""+((BulkRequest) request).requests().size();
                }

                if(request instanceof MultiGetRequest) {
                    count = ""+((MultiGetRequest) request).getItems().size();
                }

                if(request instanceof MultiSearchRequest) {
                    count = ""+((MultiSearchRequest) request).requests().size();
                }

                traceAction("Node "+cs.localNode().getName()+" -> "+action+" ("+count+"): userIsAdmin="+userIsAdmin+"/conRequest="+confRequest+"/internalRequest="+internalRequest
                        +"origin="+threadContext.getTransient(ConfigConstants.SECURITY_ORIGIN)+"/directRequest="+HeaderHelper.isDirectRequest(threadContext)+"/remoteAddress="+request.remoteAddress());


                threadContext.putHeader("_security_trace"+System.currentTimeMillis()+"#"+UUID.randomUUID().toString(), Thread.currentThread().getName()+" FILTER -> "+"Node "+cs.localNode().getName()+" -> "+action+" userIsAdmin="+userIsAdmin+"/conRequest="+confRequest+"/internalRequest="+internalRequest
                        +"origin="+threadContext.getTransient(ConfigConstants.SECURITY_ORIGIN)+"/directRequest="+HeaderHelper.isDirectRequest(threadContext)+"/remoteAddress="+request.remoteAddress()+" "+threadContext.getHeaders().entrySet().stream().filter(p->!p.getKey().startsWith("_security_trace")).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue())));


            }


            if(userIsAdmin
                    || confRequest
                    || internalRequest
                    || passThroughRequest){

                if(userIsAdmin && !confRequest && !internalRequest && !passThroughRequest) {
                    auditLog.logGrantedPrivileges(action, request, task);
                    auditLog.logIndexEvent(action, request, task);
                }

                chain.proceed(task, action, request, listener);
                return;
            }


            if(immutableIndicesMatcher != WildcardMatcher.NONE) {

                boolean isImmutable = false;

                if(request instanceof BulkShardRequest) {
                    for(BulkItemRequest bsr: ((BulkShardRequest) request).items()) {
                        isImmutable = checkImmutableIndices(bsr.request(), listener);
                        if(isImmutable) {
                            break;
                        }
                    }
                } else {
                    isImmutable = checkImmutableIndices(request, listener);
                }

                if(isImmutable) {
                    return;
                }

            }

            if(AuditLog.Origin.LOCAL.toString().equals(threadContext.getTransient(ConfigConstants.SECURITY_ORIGIN))
                    && (interClusterRequest || HeaderHelper.isDirectRequest(threadContext))
                    && (injectedRoles == null)
                    && !enforcePrivilegesEvaluation
                    ) {

                chain.proceed(task, action, request, listener);
                return;
            }

            if(user == null) {

                if(action.startsWith("cluster:monitor/state")) {
                    chain.proceed(task, action, request, listener);
                    return;
                }

                if((interClusterRequest || trustedClusterRequest || request.remoteAddress() == null) && !compatConfig.transportInterClusterAuthEnabled()) {
                    chain.proceed(task, action, request, listener);
                    return;
                }

                log.error("No user found for "+ action+" from "+request.remoteAddress()+" "+threadContext.getTransient(ConfigConstants.SECURITY_ORIGIN)+" via "+threadContext.getTransient(ConfigConstants.SECURITY_CHANNEL_TYPE)+" "+threadContext.getHeaders());
                listener.onFailure(new ElasticsearchSecurityException("No user found for "+action, RestStatus.INTERNAL_SERVER_ERROR));
                return;
            }

            final PrivilegesEvaluator eval = evalp;

            if (!eval.isInitialized()) {
                log.error("initializing for {}", action);
                listener.onFailure(new ElasticsearchSecurityException("initializing for "
                + action, RestStatus.SERVICE_UNAVAILABLE));
                return;
            }

            if (log.isTraceEnabled()) {
                log.trace("Evaluate permissions for user: {}", user.getName());
            }

            final PrivilegesEvaluatorResponse pres = eval.evaluate(user, action, request, task, injectedRoles);

            if (log.isDebugEnabled()) {
                log.debug(pres);
            }

            if (pres.isAllowed()) {
                auditLog.logGrantedPrivileges(action, request, task);
                auditLog.logIndexEvent(action, request, task);
                if(!dlsFlsValve.invoke(request, listener, pres.getAllowedFlsFields(), pres.getMaskedFields(), pres.getQueries())) {
                    return;
                }
                final CreateIndexRequest createIndexRequest = pres.getRequest();
                if (createIndexRequest == null) {
                    chain.proceed(task, action, request, listener);
                } else {
                    client.admin().indices().create(createIndexRequest, new ActionListener<CreateIndexResponse>() {
                        @Override
                        public void onResponse(CreateIndexResponse createIndexResponse) {
                            if (createIndexResponse.isAcknowledged()) {
                                log.debug("Request to create index {} with aliases {} acknowledged, proceeding with {}",
                                        createIndexRequest.index(), alias2Name(createIndexRequest.aliases()), request.getClass().getSimpleName());
                                chain.proceed(task, action, request, listener);
                            } else {
                                Exception e = new ElasticsearchException("Request to create index {} with aliases {} was not acknowledged, failing {}",
                                        createIndexRequest.index(), alias2Name(createIndexRequest.aliases()), request.getClass().getSimpleName());
                                log.error(e.getMessage());
                                listener.onFailure(e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof ResourceAlreadyExistsException) {
                                log.debug("Request to create index {} with aliases {} failed as resource already exist, proceeding with {}",
                                        createIndexRequest.index(), alias2Name(createIndexRequest.aliases()), request.getClass().getSimpleName(), e);
                                chain.proceed(task, action, request, listener);
                            } else {
                                log.error("Request to create index {} with aliases {} failed, failing {}",
                                        createIndexRequest.index(), alias2Name(createIndexRequest.aliases()), request.getClass().getSimpleName(), e);
                                listener.onFailure(e);
                            }
                        }
                    });
                }
            } else {
                    auditLog.logMissingPrivileges(action, request, task);
                    String err;
                    if (!pres.getMissingSecurityRoles().isEmpty()) {
                        err = String.format("No mapping for %s on roles %s", user, pres.getMissingSecurityRoles());
                    } else {
                        err = (injectedRoles != null) ?
                            String.format("no permissions for %s and associated roles %s", pres.getMissingPrivileges(), pres.getResolvedSecurityRoles()) :
                            String.format("no permissions for %s and %s", pres.getMissingPrivileges(), user);
                    }
                    log.debug(err);
                    listener.onFailure(new ElasticsearchSecurityException(err, RestStatus.FORBIDDEN));
            }
        } catch (ElasticsearchException e) {
            if (task != null) {
                log.debug("Failed to apply filter. Task id: {} ({}). Action: {}", task.getId(), task.getDescription(), action, e);
            } else {
                log.debug("Failed to apply filter. Action: {}", action, e);
            }
            listener.onFailure(e);
        } catch (Throwable e) {
            log.error("Unexpected exception "+e, e);
            listener.onFailure(new ElasticsearchSecurityException("Unexpected exception " + action, RestStatus.INTERNAL_SERVER_ERROR));
        }
    }

    private static boolean isUserAdmin(User user, final AdminDNs adminDns) {
        if (user != null && adminDns.isAdmin(user)) {
            return true;
        }

        return false;
    }

    private void attachSourceFieldContext(ActionRequest request) {

        if(request instanceof SearchRequest && SourceFieldsContext.isNeeded((SearchRequest) request)) {
            if(threadContext.getHeader("_security_source_field_context") == null) {
                final String serializedSourceFieldContext = Base64Helper.serializeObject(new SourceFieldsContext((SearchRequest) request));
                threadContext.putHeader("_security_source_field_context", serializedSourceFieldContext);
            }
        } else if (request instanceof GetRequest && SourceFieldsContext.isNeeded((GetRequest) request)) {
            if(threadContext.getHeader("_security_source_field_context") == null) {
                final String serializedSourceFieldContext = Base64Helper.serializeObject(new SourceFieldsContext((GetRequest) request));
                threadContext.putHeader("_security_source_field_context", serializedSourceFieldContext);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private boolean checkImmutableIndices(Object request, ActionListener listener) {
        final boolean isModifyIndexRequest = request instanceof DeleteRequest
                || request instanceof UpdateRequest
                || request instanceof UpdateByQueryRequest
                || request instanceof DeleteByQueryRequest
                || request instanceof DeleteIndexRequest
                || request instanceof RestoreSnapshotRequest
                || request instanceof CloseIndexRequest
                || request instanceof IndicesAliasesRequest;

        if (isModifyIndexRequest && isRequestIndexImmutable(request)) {
            listener.onFailure(new ElasticsearchSecurityException("Index is immutable", RestStatus.FORBIDDEN));
            return true;
        }

        if ((request instanceof IndexRequest) && isRequestIndexImmutable(request)) {
            ((IndexRequest) request).opType(OpType.CREATE);
        }

        return false;
    }

    private boolean isRequestIndexImmutable(Object request) {
        final IndexResolverReplacer.Resolved resolved = indexResolverReplacer.resolveRequest(request);
//        if (resolved.isLocalAll()) {
//            return true;
//        }
        final Set<String> allIndices = resolved.getAllIndices();

        return immutableIndicesMatcher.matchAny(allIndices);
    }
}
