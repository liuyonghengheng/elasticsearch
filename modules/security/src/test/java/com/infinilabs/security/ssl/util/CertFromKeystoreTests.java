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
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import com.infinilabs.security.test.helper.file.FileHelper;

import org.junit.Assert;
import org.junit.Test;

public class CertFromKeystoreTests {

    @Test
    public void testLoadSameCertForClientServerUsage() throws UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        KeystoreProps props = new KeystoreProps(
            FileHelper.getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks").toString(),
            "JKS",
            "changeit"
        );

        CertFromKeystore cert = new CertFromKeystore(props, "node-0", "changeit");

        // second cert is Signing cert
        Assert.assertEquals(2, cert.getCerts().length);
        Assert.assertTrue(cert.getCerts()[0].getSubjectDN().getName().contains("node-0"));

        Assert.assertNotNull(cert.getServerKey());
        Assert.assertNotNull(cert.getClientKey());
    }

    @Test
    public void testLoadSameCertWithoutAlias() throws UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        KeystoreProps props = new KeystoreProps(
            FileHelper.getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks").toString(),
            "JKS",
            "changeit"
        );

        CertFromKeystore cert = new CertFromKeystore(props, null, "changeit");

        // second cert is Signing cert
        Assert.assertEquals(2, cert.getCerts().length);
        Assert.assertTrue(cert.getCerts()[0].getSubjectDN().getName().contains("node-0"));
    }

    @Test
    public void testLoadDifferentCertsForClientServerUsage() throws UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        KeystoreProps props = new KeystoreProps(
            FileHelper.getAbsoluteFilePathFromClassPath("ssl/extended_key_usage/node-0-keystore.jks").toString(),
            "JKS",
            "changeit"
        );

        CertFromKeystore cert = new CertFromKeystore(props, "node-0-server", "node-0-client", "changeit", "changeit");

        Assert.assertEquals(4, cert.getCerts().length);

        Assert.assertTrue(cert.getClientCert()[0].getSubjectDN().getName().contains("node-client"));
        Assert.assertTrue(cert.getServerCert()[0].getSubjectDN().getName().contains("node-server"));
        Assert.assertNotNull(cert.getServerKey());
        Assert.assertNotNull(cert.getClientKey());
    }
}
