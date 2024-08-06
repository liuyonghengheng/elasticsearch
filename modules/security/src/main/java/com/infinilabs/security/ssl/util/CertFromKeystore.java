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

import org.apache.commons.lang3.ArrayUtils;
import org.elasticsearch.ElasticsearchException;

import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class CertFromKeystore {

    private final KeystoreProps keystoreProps;
    private final String serverKeystoreAlias;
    private final String clientKeystoreAlias;

    private PrivateKey serverKey;
    private X509Certificate[] serverCert;
    private final char[] serverKeyPassword;

    private PrivateKey clientKey;
    private X509Certificate[] clientCert;
    private final char[] clientKeyPassword;

    private X509Certificate[] loadedCerts;

    public CertFromKeystore(KeystoreProps keystoreProps, String keystoreAlias, String keyPassword) throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, UnrecoverableKeyException {
        this.keystoreProps = keystoreProps;
        final KeyStore ks = keystoreProps.loadKeystore();

        this.serverKeystoreAlias = keystoreAlias;
        this.serverKeyPassword = Utils.toCharArray(keyPassword);
        this.serverCert = SSLCertificateHelper.exportServerCertChain(ks, serverKeystoreAlias);
        this.serverKey = SSLCertificateHelper.exportDecryptedKey(
                ks, serverKeystoreAlias, this.serverKeyPassword);

        this.clientKeystoreAlias = keystoreAlias;
        this.clientKeyPassword = serverKeyPassword;
        this.clientCert = serverCert;
        this.clientKey = serverKey;

        this.loadedCerts = serverCert;

        validate();
    }

    public CertFromKeystore(
            KeystoreProps keystoreProps,
            String serverKeystoreAlias, String clientKeystoreAlias, String serverKeyPassword, String clientKeyPassword) throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, UnrecoverableKeyException {
        this.keystoreProps = keystoreProps;
        final KeyStore ks = keystoreProps.loadKeystore();

        this.serverKeystoreAlias = serverKeystoreAlias;
        this.serverKeyPassword = Utils.toCharArray(serverKeyPassword);
        this.serverCert = SSLCertificateHelper.exportServerCertChain(ks, serverKeystoreAlias);
        this.serverKey = SSLCertificateHelper.exportDecryptedKey(
                ks, serverKeystoreAlias, this.serverKeyPassword);

        this.clientKeystoreAlias = clientKeystoreAlias;
        this.clientKeyPassword = Utils.toCharArray(clientKeyPassword);
        this.clientCert = SSLCertificateHelper.exportServerCertChain(ks, clientKeystoreAlias);
        this.clientKey = SSLCertificateHelper.exportDecryptedKey(
                ks, clientKeystoreAlias, this.clientKeyPassword);

        this.loadedCerts = ArrayUtils.addAll(serverCert, clientCert);

        validate();
    }

    private void validate() {
        if (serverKey == null) {
            throw new ElasticsearchException(
                    "No key found in " + keystoreProps.getFilePath() + " with alias " + serverKeystoreAlias);
        }

        if (serverCert == null || serverCert.length == 0) {
            throw new ElasticsearchException(
                    "No certificates found in " + keystoreProps.getFilePath() + " with alias " + serverKeystoreAlias);
        }

        if (clientKey == null) {
            throw new ElasticsearchException(
                    "No key found in " + keystoreProps.getFilePath() + " with alias " + clientKeystoreAlias);
        }

        if (clientCert == null || clientCert.length == 0) {
            throw new ElasticsearchException(
                    "No certificates found in " + keystoreProps.getFilePath() + " with alias " + clientKeystoreAlias);
        }
    }

    public X509Certificate[] getCerts() {
        return loadedCerts;
    }

    public PrivateKey getServerKey() {
        return serverKey;
    }

    public X509Certificate[] getServerCert() {
        return serverCert;
    }

    public PrivateKey getClientKey() {
        return clientKey;
    }

    public X509Certificate[] getClientCert() {
        return clientCert;
    }
}
