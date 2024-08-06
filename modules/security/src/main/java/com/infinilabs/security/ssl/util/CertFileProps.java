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


public class CertFileProps {
  private final String pemCertFilePath;
  private final String pemKeyFilePath;
  private final String trustedCasFilePath;
  private final String pemKeyPassword;

  public CertFileProps(String pemCertFilePath, String pemKeyFilePath, String trustedCasFilePath, String pemKeyPassword) {
    this.pemCertFilePath = pemCertFilePath;
    this.pemKeyFilePath = pemKeyFilePath;
    this.trustedCasFilePath = trustedCasFilePath;
    this.pemKeyPassword = pemKeyPassword;
  }

  public String getPemCertFilePath() {
    return pemCertFilePath;
  }

  public String getPemKeyFilePath() {
    return pemKeyFilePath;
  }

  public String getTrustedCasFilePath() {
    return trustedCasFilePath;
  }

  public String getPemKeyPassword() {
    return pemKeyPassword;
  }
}
