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
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.infinilabs.security.dlic.dlsfls;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.infinilabs.security.test.helper.rest.RestHelper;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class FlsPerfTest extends AbstractDlsFlsTest{


    protected void populateData(TransportClient tc) {

        Map<String, Object> indexSettings = new HashMap<>(3);
        indexSettings.put("index.mapping.total_fields.limit",50000);
        indexSettings.put("number_of_shards", 10);
        indexSettings.put("number_of_replicas", 0);

        tc.admin().indices().create(new CreateIndexRequest("deals")
        .settings(indexSettings))
        .actionGet();

        try {

            IndexRequest ir =  new IndexRequest("deals").type("deals2").id("idx1");
            XContentBuilder b = XContentBuilder.builder(JsonXContent.jsonXContent);
            b.startObject();

            b.field("amount",1000);

            b.startObject("xyz");
            b.field("abc","val");
            b.endObject();

            b.endObject();
            ir.source(b);

            tc.index(ir).actionGet();

            for(int i=0; i<1500; i++) {

                ir =  new IndexRequest("deals").type("deals").id("id"+i);
                b = XContentBuilder.builder(JsonXContent.jsonXContent);
                b.startObject();
                for(int j=0; j<2000;j++) {
                    b.field("field"+j,"val"+j);
                }

                b.endObject();
                ir.source(b);

                tc.index(ir).actionGet();

            }

            tc.admin().indices().refresh(new RefreshRequest("deals")).actionGet();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.toString());
        }

    }

    @Test
    public void testFlsPerfNamed() throws Exception {

        setup();

        RestHelper.HttpResponse res;

        StopWatch sw = new StopWatch("testFlsPerfNamed");
        sw.start("non field_security");
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        sw.stop();
        Assert.assertTrue(res.getBody().contains("field1\""));
        Assert.assertTrue(res.getBody().contains("field2\""));
        Assert.assertTrue(res.getBody().contains("field50\""));
        Assert.assertTrue(res.getBody().contains("field997\""));

        sw.start("with field_security");
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=1000", encodeBasicHeader("perf_named_only", "password"))).getStatusCode());
        sw.stop();
        Assert.assertFalse(res.getBody().contains("field1\""));
        Assert.assertFalse(res.getBody().contains("field2\""));
        Assert.assertTrue(res.getBody().contains("field50\""));
        Assert.assertTrue(res.getBody().contains("field997\""));

        sw.start("with field_security 2 after warmup");

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=1000", encodeBasicHeader("perf_named_only", "password"))).getStatusCode());
        sw.stop();

        Assert.assertFalse(res.getBody().contains("field1\""));
        Assert.assertFalse(res.getBody().contains("field2\""));
        Assert.assertTrue(res.getBody().contains("field50\""));
        Assert.assertTrue(res.getBody().contains("field997\""));

        sw.start("with field_security 3 after warmup");

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=1000", encodeBasicHeader("perf_named_only", "password"))).getStatusCode());
        sw.stop();

        Assert.assertFalse(res.getBody().contains("field1\""));
        Assert.assertFalse(res.getBody().contains("field2\""));
        Assert.assertTrue(res.getBody().contains("field50\""));
        Assert.assertTrue(res.getBody().contains("field997\""));

        System.out.println(sw.prettyPrint());
    }

    @Test
    public void testFlsPerfWcEx() throws Exception {

        setup();

        RestHelper.HttpResponse res;

        StopWatch sw = new StopWatch("testFlsPerfWcEx");
        sw.start("non field_security");
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        sw.stop();
        Assert.assertTrue(res.getBody().contains("field1\""));
        Assert.assertTrue(res.getBody().contains("field2\""));
        Assert.assertTrue(res.getBody().contains("field50\""));
        Assert.assertTrue(res.getBody().contains("field997\""));

        sw.start("with field_security");
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=1000", encodeBasicHeader("perf_wc_ex", "password"))).getStatusCode());
        sw.stop();
        Assert.assertTrue(res.getBody().contains("field1\""));
        Assert.assertTrue(res.getBody().contains("field2\""));
        Assert.assertFalse(res.getBody().contains("field50\""));
        Assert.assertFalse(res.getBody().contains("field997\""));

        sw.start("with field_security 2 after warmup");

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=1000", encodeBasicHeader("perf_wc_ex", "password"))).getStatusCode());
        sw.stop();

        Assert.assertTrue(res.getBody().contains("field1\""));
        Assert.assertTrue(res.getBody().contains("field2\""));
        Assert.assertFalse(res.getBody().contains("field50\""));
        Assert.assertFalse(res.getBody().contains("field997\""));

        sw.start("with field_security 3 after warmup");

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=1000", encodeBasicHeader("perf_wc_ex", "password"))).getStatusCode());
        sw.stop();

        Assert.assertTrue(res.getBody().contains("field1\""));
        Assert.assertTrue(res.getBody().contains("field2\""));
        Assert.assertFalse(res.getBody().contains("field50\""));
        Assert.assertFalse(res.getBody().contains("field997\""));

        System.out.println(sw.prettyPrint());
    }

    @Test
    public void testFlsPerfNamedEx() throws Exception {

        setup();

        RestHelper.HttpResponse res;

        StopWatch sw = new StopWatch("testFlsPerfNamedEx");
        sw.start("non field_security");
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        sw.stop();
        Assert.assertTrue(res.getBody().contains("field1\""));
        Assert.assertTrue(res.getBody().contains("field2\""));
        Assert.assertTrue(res.getBody().contains("field50\""));
        Assert.assertTrue(res.getBody().contains("field997\""));

        sw.start("with field_security");
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=1000", encodeBasicHeader("perf_named_ex", "password"))).getStatusCode());
        sw.stop();
        Assert.assertTrue(res.getBody().contains("field1\""));
        Assert.assertTrue(res.getBody().contains("field2\""));
        Assert.assertFalse(res.getBody().contains("field50\""));
        Assert.assertFalse(res.getBody().contains("field997\""));

        sw.start("with field_security 2 after warmup");

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=1000", encodeBasicHeader("perf_named_ex", "password"))).getStatusCode());
        sw.stop();

        Assert.assertTrue(res.getBody().contains("field1\""));
        Assert.assertTrue(res.getBody().contains("field2\""));
        Assert.assertFalse(res.getBody().contains("field50\""));
        Assert.assertFalse(res.getBody().contains("field997\""));

        sw.start("with field_security 3 after warmup");

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=1000", encodeBasicHeader("perf_named_ex", "password"))).getStatusCode());
        sw.stop();

        Assert.assertTrue(res.getBody().contains("field1\""));
        Assert.assertTrue(res.getBody().contains("field2\""));
        Assert.assertFalse(res.getBody().contains("field50\""));
        Assert.assertFalse(res.getBody().contains("field997\""));

        System.out.println(sw.prettyPrint());
    }

    @Test
    public void testFlsWcIn() throws Exception {

        setup();

        RestHelper.HttpResponse res;

        StopWatch sw = new StopWatch("testFlsWcIn");
        sw.start("non field_security");
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty", encodeBasicHeader("admin", "admin"))).getStatusCode());
        sw.stop();
        Assert.assertTrue(res.getBody().contains("field1\""));
        Assert.assertTrue(res.getBody().contains("field2\""));
        Assert.assertTrue(res.getBody().contains("field50\""));
        Assert.assertTrue(res.getBody().contains("field997\""));

        sw.start("with field_security");
        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=1000", encodeBasicHeader("perf_wc_in", "password"))).getStatusCode());
        sw.stop();
        Assert.assertFalse(res.getBody().contains("field0\""));
        Assert.assertTrue(res.getBody().contains("field50\""));
        Assert.assertTrue(res.getBody().contains("field997\""));

        sw.start("with field_security 2 after warmup");

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=1000", encodeBasicHeader("perf_wc_in", "password"))).getStatusCode());
        sw.stop();

        Assert.assertFalse(res.getBody().contains("field0\""));
        Assert.assertTrue(res.getBody().contains("field50\""));
        Assert.assertTrue(res.getBody().contains("field997\""));

        sw.start("with field_security 3 after warmup");

        Assert.assertEquals(HttpStatus.SC_OK, (res = rh.executeGetRequest("/deals/_search?pretty&size=1000", encodeBasicHeader("perf_wc_in", "password"))).getStatusCode());
        sw.stop();

        Assert.assertFalse(res.getBody().contains("field0\""));
        Assert.assertTrue(res.getBody().contains("field50\""));
        Assert.assertTrue(res.getBody().contains("field997\""));

        System.out.println(sw.prettyPrint());
    }
}
