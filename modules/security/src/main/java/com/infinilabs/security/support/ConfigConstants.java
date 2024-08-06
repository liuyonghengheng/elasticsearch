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

package com.infinilabs.security.support;

import com.infinilabs.security.auditlog.impl.AuditCategory;

import org.elasticsearch.common.settings.Settings;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class ConfigConstants {


    public static final String SECURITY_CONFIG_PREFIX = "_security_";

    public static final String SECURITY_CHANNEL_TYPE = SECURITY_CONFIG_PREFIX+"channel_type";

    public static final String SECURITY_ORIGIN = SECURITY_CONFIG_PREFIX+"origin";
    public static final String SECURITY_ORIGIN_HEADER = SECURITY_CONFIG_PREFIX+"origin_header";

    public static final String SECURITY_DLS_QUERY_HEADER = SECURITY_CONFIG_PREFIX+"dls_query";

    public static final String SECURITY_FLS_FIELDS_HEADER = SECURITY_CONFIG_PREFIX+"fls_fields";

    public static final String SECURITY_MASKED_FIELD_HEADER = SECURITY_CONFIG_PREFIX+"field_mask";

    public static final String SECURITY_DLS_QUERY_CCS = SECURITY_CONFIG_PREFIX+"dls_query_ccs";

    public static final String SECURITY_FLS_FIELDS_CCS = SECURITY_CONFIG_PREFIX+"fls_fields_ccs";

    public static final String SECURITY_MASKED_FIELD_CCS = SECURITY_CONFIG_PREFIX+"masked_fields_ccs";

    public static final String SECURITY_CONF_REQUEST_HEADER = SECURITY_CONFIG_PREFIX+"conf_request";

    public static final String SECURITY_REMOTE_ADDRESS = SECURITY_CONFIG_PREFIX+"remote_address";
    public static final String SECURITY_REMOTE_ADDRESS_HEADER = SECURITY_CONFIG_PREFIX+"remote_address_header";

    public static final String SECURITY_INITIAL_ACTION_CLASS_HEADER = SECURITY_CONFIG_PREFIX+"initial_action_class_header";

    /**
     * Set by SSL plugin for https requests only
     */
    public static final String SECURITY_SSL_PEER_CERTIFICATES = SECURITY_CONFIG_PREFIX+"ssl_peer_certificates";

    /**
     * Set by SSL plugin for https requests only
     */
    public static final String SECURITY_SSL_PRINCIPAL = SECURITY_CONFIG_PREFIX+"ssl_principal";

    /**
     * If this is set to TRUE then the request comes from a Server Node (fully trust)
     * Its expected that there is a _security_user attached as header
     */
    public static final String SECURITY_SSL_TRANSPORT_INTERCLUSTER_REQUEST = SECURITY_CONFIG_PREFIX+"ssl_transport_intercluster_request";

    public static final String SECURITY_SSL_TRANSPORT_TRUSTED_CLUSTER_REQUEST = SECURITY_CONFIG_PREFIX+"ssl_transport_trustedcluster_request";


    /**
     * Set by the SSL plugin, this is the peer node certificate on the transport layer
     */
    public static final String SECURITY_SSL_TRANSPORT_PRINCIPAL = SECURITY_CONFIG_PREFIX+"ssl_transport_principal";

    public static final String SECURITY_USER = SECURITY_CONFIG_PREFIX+"user";
    public static final String SECURITY_USER_HEADER = SECURITY_CONFIG_PREFIX+"user_header";

    public static final String SECURITY_USER_INFO_THREAD_CONTEXT = SECURITY_CONFIG_PREFIX + "user_info";

    public static final String SECURITY_INJECTED_USER = "injected_user";
    public static final String SECURITY_INJECTED_USER_HEADER = "injected_user_header";

    public static final String SECURITY_XFF_DONE = SECURITY_CONFIG_PREFIX+"xff_done";

    public static final String SSO_LOGOUT_URL = SECURITY_CONFIG_PREFIX+"sso_logout_url";


    public static final String SECURITY_DEFAULT_CONFIG_INDEX = ".security";

    public static final String SECURITY_ENABLE_SNAPSHOT_RESTORE_PRIVILEGE = "security.enable_snapshot_restore_privilege";
    public static final boolean SECURITY_DEFAULT_ENABLE_SNAPSHOT_RESTORE_PRIVILEGE = true;

    public static final String SECURITY_CHECK_SNAPSHOT_RESTORE_WRITE_PRIVILEGES = "security.check_snapshot_restore_write_privileges";
    public static final boolean SECURITY_DEFAULT_CHECK_SNAPSHOT_RESTORE_WRITE_PRIVILEGES = true;
    public static final Set<String> SECURITY_SNAPSHOT_RESTORE_NEEDED_WRITE_PRIVILEGES = Collections.unmodifiableSet(
            new HashSet<String>(Arrays.asList(
                    "indices:admin/create",
                    "indices:data/write/index"
                    // "indices:data/write/bulk"
              )));

    public static final String SECURITY_INTERCLUSTER_REQUEST_EVALUATOR_CLASS = "security.cert.intercluster_request_evaluator_class";
    public static final String SECURITY_ACTION_NAME = SECURITY_CONFIG_PREFIX+"action_name";


    public static final String SECURITY_AUTHCZ_ADMIN_DN = "security.authcz.admin_dn";
    public static final String SECURITY_CONFIG_INDEX_NAME = "security.config_index_name";
    public static final String SECURITY_AUTHCZ_IMPERSONATION_DN = "security.authcz.impersonation_dn";
    public static final String SECURITY_AUTHCZ_REST_IMPERSONATION_USERS="security.authcz.rest_impersonation_user";

    public static final String SECURITY_AUDIT_TYPE_DEFAULT = "security.audit.type";
    public static final String SECURITY_AUDIT_CONFIG_DEFAULT = "security.audit.config";
    public static final String SECURITY_AUDIT_CONFIG_ROUTES = "security.audit.routes";
    public static final String SECURITY_AUDIT_CONFIG_ENDPOINTS = "security.audit.endpoints";
    public static final String SECURITY_AUDIT_THREADPOOL_SIZE = "security.audit.threadpool.size";
    public static final String SECURITY_AUDIT_THREADPOOL_MAX_QUEUE_LEN = "security.audit.threadpool.max_queue_len";
    public static final String SECURITY_AUDIT_LOG_REQUEST_BODY = "security.audit.log_request_body";
    public static final String SECURITY_AUDIT_RESOLVE_INDICES = "security.audit.resolve_indices";
    public static final String SECURITY_AUDIT_ENABLE_REST = "security.audit.enable_rest";
    public static final String SECURITY_AUDIT_ENABLE_TRANSPORT = "security.audit.enable_transport";
    public static final String SECURITY_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES = "security.audit.config.disabled_transport_categories";
    public static final String SECURITY_AUDIT_CONFIG_DISABLED_REST_CATEGORIES = "security.audit.config.disabled_rest_categories";
    public static final List<String> SECURITY_AUDIT_DISABLED_CATEGORIES_DEFAULT = ImmutableList.of(AuditCategory.AUTHENTICATED.toString(),
            AuditCategory.GRANTED_PRIVILEGES.toString());
    public static final String SECURITY_AUDIT_IGNORE_USERS = "security.audit.ignore_users";
    public static final String SECURITY_AUDIT_IGNORE_REQUESTS = "security.audit.ignore_requests";
    public static final String SECURITY_AUDIT_RESOLVE_BULK_REQUESTS = "security.audit.resolve_bulk_requests";
    public static final boolean SECURITY_AUDIT_SSL_VERIFY_HOSTNAMES_DEFAULT = true;
    public static final boolean SECURITY_AUDIT_SSL_ENABLE_SSL_CLIENT_AUTH_DEFAULT = false;
    public static final String SECURITY_AUDIT_EXCLUDE_SENSITIVE_HEADERS = "security.audit.exclude_sensitive_headers";

    public static final String SECURITY_AUDIT_CONFIG_DEFAULT_PREFIX = "security.audit.config.";

    // Internal / External ES
    public static final String SECURITY_AUDIT_ES_INDEX = "index";
    public static final String SECURITY_AUDIT_ES_TYPE = "type";

    // External ES
    public static final String SECURITY_AUDIT_EXTERNAL_ES_HTTP_ENDPOINTS = "http_endpoints";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_USERNAME = "username";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_PASSWORD = "password";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_ENABLE_SSL = "enable_ssl";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_VERIFY_HOSTNAMES = "verify_hostnames";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_ENABLE_SSL_CLIENT_AUTH = "enable_ssl_client_auth";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_PEMKEY_FILEPATH = "key_file";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_PEMKEY_CONTENT = "key_file_content";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_PEMKEY_PASSWORD = "key_secret";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_PEMCERT_FILEPATH = "cert_file";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_PEMCERT_CONTENT = "cert_file_content";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_PEMTRUSTEDCAS_FILEPATH = "ca_file";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_PEMTRUSTEDCAS_CONTENT = "ca_file_content";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_JKS_CERT_ALIAS = "cert_alias";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_ENABLED_SSL_CIPHERS = "enabled_ssl_ciphers";
    public static final String SECURITY_AUDIT_EXTERNAL_ES_ENABLED_SSL_PROTOCOLS = "enabled_ssl_protocols";

    // Webhooks
    public static final String SECURITY_AUDIT_WEBHOOK_URL = "webhook.url";
    public static final String SECURITY_AUDIT_WEBHOOK_FORMAT = "webhook.format";
    public static final String SECURITY_AUDIT_WEBHOOK_SSL_VERIFY = "webhook.ssl.verify";
    public static final String SECURITY_AUDIT_WEBHOOK_PEMTRUSTEDCAS_FILEPATH = "webhook.ssl.ca_file";
    public static final String SECURITY_AUDIT_WEBHOOK_PEMTRUSTEDCAS_CONTENT = "webhook.ssl.ca_file_content";

    // Log4j
    public static final String SECURITY_AUDIT_LOG4J_LOGGER_NAME = "log4j.logger_name";
    public static final String SECURITY_AUDIT_LOG4J_LEVEL = "log4j.level";

    //retry
    public static final String SECURITY_AUDIT_RETRY_COUNT = "security.audit.config.retry_count";
    public static final String SECURITY_AUDIT_RETRY_DELAY_MS = "security.audit.config.retry_delay_ms";


    public static final String SECURITY_KERBEROS_KRB5_FILEPATH = "security.kerberos.krb5_filepath";
    public static final String SECURITY_KERBEROS_ACCEPTOR_KEYTAB_FILEPATH = "security.kerberos.acceptor_keytab_filepath";
    public static final String SECURITY_KERBEROS_ACCEPTOR_PRINCIPAL = "security.kerberos.acceptor_principal";
    public static final String SECURITY_CERT_OID = "security.cert.oid";
    public static final String SECURITY_CERT_INTERCLUSTER_REQUEST_EVALUATOR_CLASS = "security.cert.intercluster_request_evaluator_class";
    public static final String SECURITY_ADVANCED_MODULES_ENABLED = "security.advanced_modules_enabled";
    public static final String SECURITY_NODES_DN = "security.nodes_dn";
    public static final String SECURITY_NODES_DN_DYNAMIC_CONFIG_ENABLED = "security.nodes_dn_dynamic_config_enabled";
    public static final String SECURITY_ENABLED = "security.enabled";
    public static final String SECURITY_CACHE_TTL_MINUTES = "security.cache.ttl_minutes";
    public static final String SECURITY_ALLOW_DEFAULT_INIT_SECURITYINDEX = "security.allow_default_init_securityindex";
    public static final String SECURITY_BACKGROUND_INIT_IF_SECURITYINDEX_NOT_EXIST = "security.background_init_if_securityindex_not_exist";

    public static final String SECURITY_ROLES_MAPPING_RESOLUTION = "security.role_mapping_resolution";

    public static final String SECURITY_COMPLIANCE_HISTORY_WRITE_METADATA_ONLY = "security.compliance.history.write.metadata_only";
    public static final String SECURITY_COMPLIANCE_HISTORY_READ_METADATA_ONLY = "security.compliance.history.read.metadata_only";
    public static final String SECURITY_COMPLIANCE_HISTORY_READ_WATCHED_FIELDS = "security.compliance.history.read.watched_fields";
    public static final String SECURITY_COMPLIANCE_HISTORY_WRITE_WATCHED_INDICES = "security.compliance.history.write.watched_indices";
    public static final String SECURITY_COMPLIANCE_HISTORY_WRITE_LOG_DIFFS = "security.compliance.history.write.log_diffs";
    public static final String SECURITY_COMPLIANCE_HISTORY_READ_IGNORE_USERS = "security.compliance.history.read.ignore_users";
    public static final String SECURITY_COMPLIANCE_HISTORY_WRITE_IGNORE_USERS = "security.compliance.history.write.ignore_users";
    public static final String SECURITY_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED  = "security.compliance.history.external_config_enabled";
    public static final String SECURITY_COMPLIANCE_DISABLE_ANONYMOUS_AUTHENTICATION  = "security.compliance.disable_anonymous_authentication";
    public static final String SECURITY_COMPLIANCE_IMMUTABLE_INDICES = "security.compliance.immutable_indices";
    public static final String SECURITY_COMPLIANCE_SALT = "security.compliance.salt";
    public static final String SECURITY_COMPLIANCE_SALT_DEFAULT = "e1ukloTxxxOgPquJ";//16 chars
    public static final String SECURITY_COMPLIANCE_HISTORY_INTERNAL_CONFIG_ENABLED  = "security.compliance.history.internal_config_enabled";
    public static final String SECURITY_SSL_ONLY = "security.ssl_only";
    public static final String SECURITY_CONFIG_SSL_DUAL_MODE_ENABLED = "security_config.ssl_dual_mode_enabled";
    public static final String SECURITY_SSL_CERT_RELOAD_ENABLED = "security.ssl_cert_reload_enabled";
    public static final String SECURITY_DISABLE_ENVVAR_REPLACEMENT = "security.disable_envvar_replacement";

    public enum RolesMappingResolution {
        MAPPING_ONLY,
        BACKENDROLES_ONLY,
        BOTH
    }

    public static final String SECURITY_FILTER_SECURITYINDEX_FROM_ALL_REQUESTS = "security.filter_securityindex_from_all_requests";

    // REST API
    public static final String SECURITY_RESTAPI_ROLES_ENABLED = "security.restapi.roles_enabled";
    public static final String SECURITY_RESTAPI_ENDPOINTS_DISABLED = "security.restapi.endpoints_disabled";
    public static final String SECURITY_RESTAPI_PASSWORD_VALIDATION_REGEX = "security.restapi.password_validation_regex";
    public static final String SECURITY_RESTAPI_PASSWORD_VALIDATION_ERROR_MESSAGE = "security.restapi.password_validation_error_message";

    // Illegal Opcodes from here on
    public static final String SECURITY_UNSUPPORTED_DISABLE_REST_AUTH_INITIALLY = "security.unsupported.disable_rest_auth_initially";
    public static final String SECURITY_UNSUPPORTED_DISABLE_INTERTRANSPORT_AUTH_INITIALLY = "security.unsupported.disable_intertransport_auth_initially";
    public static final String SECURITY_UNSUPPORTED_RESTORE_SECURITYINDEX_ENABLED = "security.unsupported.restore.securityindex.enabled";
    public static final String SECURITY_UNSUPPORTED_INJECT_USER_ENABLED = "security.unsupported.inject_user.enabled";
    public static final String SECURITY_UNSUPPORTED_INJECT_ADMIN_USER_ENABLED = "security.unsupported.inject_user.admin.enabled";
    public static final String SECURITY_UNSUPPORTED_ALLOW_NOW_IN_DLS = "security.unsupported.allow_now_in_dls";

    public static final String SECURITY_UNSUPPORTED_RESTAPI_ALLOW_SECURITYCONFIG_MODIFICATION = "security.unsupported.restapi.allow_securityconfig_modification";
    public static final String SECURITY_UNSUPPORTED_LOAD_STATIC_RESOURCES = "security.unsupported.load_static_resources";
    public static final String SECURITY_UNSUPPORTED_ACCEPT_INVALID_CONFIG = "security.unsupported.accept_invalid_config";

    // Roles injection for plugins
    public static final String SECURITY_INJECTED_ROLES = "security_injected_roles";
    public static final String SECURITY_INJECTED_ROLES_HEADER = "security_injected_roles_header";

    // Roles validation for the plugins
    public static final String SECURITY_INJECTED_ROLES_VALIDATION = "security_injected_roles_validation";
    public static final String SECURITY_INJECTED_ROLES_VALIDATION_HEADER = "security_injected_roles_validation_header";

    // System indices settings
    public static final String SECURITY_SYSTEM_INDICES_ENABLED_KEY = "security.system_indices.enabled";
    public static final Boolean SECURITY_SYSTEM_INDICES_ENABLED_DEFAULT = false;
    public static final String SECURITY_SYSTEM_INDICES_KEY = "security.system_indices.indices";
    public static final List<String> SECURITY_SYSTEM_INDICES_DEFAULT = Collections.emptyList();

    public static Set<String> getSettingAsSet(final Settings settings, final String key, final List<String> defaultList, final boolean ignoreCaseForNone) {
        final List<String> list = settings.getAsList(key, defaultList);
        if (list.size() == 1 && "NONE".equals(ignoreCaseForNone? list.get(0).toUpperCase() : list.get(0))) {
            return Collections.emptySet();
        }
        return ImmutableSet.copyOf(list);
    }
}
