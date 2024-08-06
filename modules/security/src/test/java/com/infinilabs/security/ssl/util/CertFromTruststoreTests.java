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

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import com.infinilabs.security.test.helper.file.FileHelper;

import org.junit.Assert;
import org.junit.Test;

public class CertFromTruststoreTests {

    @Test
    public void testLoadSameCertForClientServerUsage() throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        KeystoreProps props = new KeystoreProps(
            FileHelper.getAbsoluteFilePathFromClassPath("ssl/extended_key_usage/truststore.jks").toString(),
            "JKS",
            "changeit"
        );

        CertFromTruststore cert = new CertFromTruststore(props, "root-ca");

        Assert.assertEquals(1, cert.getClientTrustedCerts().length);
        Assert.assertTrue(cert.getClientTrustedCerts().equals(cert.getServerTrustedCerts()));
    }

    @Test
    public void testLoadSameCertWithoutAlias() throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        KeystoreProps props = new KeystoreProps(
            FileHelper.getAbsoluteFilePathFromClassPath("ssl/extended_key_usage/truststore.jks").toString(),
            "JKS",
            "changeit"
        );

        CertFromTruststore cert = new CertFromTruststore(props, null);

        Assert.assertEquals(1, cert.getClientTrustedCerts().length);
    }

    public void testLoadDifferentCertsForClientServerUsage() throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        KeystoreProps props = new KeystoreProps(
            FileHelper.getAbsoluteFilePathFromClassPath("ssl/extended_key_usage/truststore.jks").toString(),
            "JKS",
            "changeit"
        );

        CertFromTruststore cert = new CertFromTruststore(props, "root-ca", "root-ca");

        Assert.assertEquals(1, cert.getClientTrustedCerts().length);
        Assert.assertEquals(1, cert.getServerTrustedCerts().length);
        // we are loading same cert twice
        Assert.assertFalse(cert.getClientTrustedCerts().equals(cert.getServerTrustedCerts()));
    }
}
