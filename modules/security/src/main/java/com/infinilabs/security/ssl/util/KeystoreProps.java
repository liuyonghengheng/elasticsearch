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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class KeystoreProps {
    private final String filePath;
    private final String type;
    private final char[] password;

    public KeystoreProps(String filePath, String type, String password) {
        this.filePath = filePath;
        this.type = type;
        this.password = Utils.toCharArray(password);
    }

    public String getFilePath() {
        return filePath;
    }

    public String getType() {
        return type;
    }

    public char[] getPassword() {
        return password;
    }

    public KeyStore loadKeystore() throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
        final KeyStore ts = KeyStore.getInstance(type);
        ts.load(new FileInputStream(new File(filePath)), password);
        return ts;
    }
}
