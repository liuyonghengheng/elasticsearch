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

import java.io.FileNotFoundException;

import com.infinilabs.security.test.helper.file.FileHelper;

import org.junit.Assert;
import org.junit.Test;

public class CertFromFileTests {

    @Test
    public void testLoadSameCertForClientServerUsage() throws Exception {
      CertFileProps certProps = new CertFileProps(
          FileHelper.getAbsoluteFilePathFromClassPath("ssl/node-0.crt.pem").toString(),
          FileHelper.getAbsoluteFilePathFromClassPath("ssl/node-0.key.pem").toString(),
          FileHelper.getAbsoluteFilePathFromClassPath("ssl/root-ca.pem").toString(),
          null);

      CertFromFile cert = new CertFromFile(certProps);

      Assert.assertEquals(1, cert.getCerts().length);
      Assert.assertNotNull(cert.getClientPemCert());
      Assert.assertNotNull(cert.getClientPemKey());
      Assert.assertNotNull(cert.getClientTrustedCas());
    }

  @Test
  public void testLoadCertWithoutCA() throws Exception {
        CertFileProps certProps = new CertFileProps(
            FileHelper.getAbsoluteFilePathFromClassPath("ssl/node-0.crt.pem").toString(),
            FileHelper.getAbsoluteFilePathFromClassPath("ssl/node-0.key.pem").toString(),
            null,
            null);

        CertFromFile cert = new CertFromFile(certProps);

        Assert.assertNull(cert.getClientTrustedCas());
    }

    @Test(expected= FileNotFoundException.class)
    public void testLoadCertWithMissingFiles() throws Exception {
        CertFileProps certProps = new CertFileProps(
            "missing.pem",
            FileHelper.getAbsoluteFilePathFromClassPath("ssl/node-0.key.pem").toString(),
            null,
            null);

        CertFromFile cert = new CertFromFile(certProps);
    }

    @Test
    public void testLoadDifferentCertsForClientServerUsage() throws Exception {
      CertFileProps clientCertProps = new CertFileProps(
          FileHelper.getAbsoluteFilePathFromClassPath("ssl/extended_key_usage/node-client.pem").toString(),
          FileHelper.getAbsoluteFilePathFromClassPath("ssl/extended_key_usage/node-key-client.pem").toString(),
          FileHelper.getAbsoluteFilePathFromClassPath("ssl/extended_key_usage/root-ca.pem").toString(),
          null);
      CertFileProps servertCertProps = new CertFileProps(
          FileHelper.getAbsoluteFilePathFromClassPath("ssl/extended_key_usage/node-server.pem").toString(),
          FileHelper.getAbsoluteFilePathFromClassPath("ssl/extended_key_usage/node-key-server.pem").toString(),
          FileHelper.getAbsoluteFilePathFromClassPath("ssl/extended_key_usage/root-ca.pem").toString(),
          null);

      CertFromFile cert = new CertFromFile(clientCertProps, servertCertProps);

      Assert.assertEquals(2, cert.getCerts().length);
    }

}
