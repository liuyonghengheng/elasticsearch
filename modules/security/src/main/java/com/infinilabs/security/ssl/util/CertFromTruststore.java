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

package com.infinilabs.security.ssl.util;

import org.elasticsearch.ElasticsearchException;

import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class CertFromTruststore {
    private final KeystoreProps keystoreProps;

    private final String serverTruststoreAlias;
    private final X509Certificate[] serverTrustedCerts;

    private final String clientTruststoreAlias;
    private final X509Certificate[] clientTrustedCerts;

    public CertFromTruststore() {
        keystoreProps = null;
        serverTruststoreAlias = null;
        serverTrustedCerts = null;
        clientTruststoreAlias = null;
        clientTrustedCerts = null;
    }

    public static CertFromTruststore Empty() {
        return new CertFromTruststore();
    }

    public CertFromTruststore(KeystoreProps keystoreProps, String truststoreAlias) throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        this.keystoreProps = keystoreProps;
        final KeyStore ts = keystoreProps.loadKeystore();

        serverTruststoreAlias = truststoreAlias;
        serverTrustedCerts = SSLCertificateHelper.exportRootCertificates(ts, truststoreAlias);

        clientTruststoreAlias = serverTruststoreAlias;
        clientTrustedCerts = serverTrustedCerts;

        validate();
    }

    public CertFromTruststore(KeystoreProps keystoreProps, String serverTruststoreAlias, String clientTruststoreAlias) throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        this.keystoreProps = keystoreProps;
        final KeyStore ts = this.keystoreProps.loadKeystore();

        this.serverTruststoreAlias = serverTruststoreAlias;
        serverTrustedCerts = SSLCertificateHelper.exportRootCertificates(ts, this.serverTruststoreAlias);

        this.clientTruststoreAlias = clientTruststoreAlias;
        clientTrustedCerts = SSLCertificateHelper.exportRootCertificates(ts, this.clientTruststoreAlias);

        validate();
    }

    private void validate() {
        if (serverTrustedCerts == null || serverTrustedCerts.length == 0) {
            throw new ElasticsearchException("No truststore configured for server certs");
        }

        if (clientTrustedCerts == null || clientTrustedCerts.length == 0) {
            throw new ElasticsearchException("No truststore configured for client certs");
        }
    }

    public X509Certificate[] getServerTrustedCerts() {
        return serverTrustedCerts;
    }

    public X509Certificate[] getClientTrustedCerts() {
        return clientTrustedCerts;
    }
}
