/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;

public class RemoveDuplicatesFilterFactoryTests extends ESTokenStreamTestCase {

    public void testRemoveDuplicatesFilter() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.removedups.type", "remove_duplicates")
            .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("removedups");
        assertThat(tokenFilter, instanceOf(RemoveDuplicatesTokenFilterFactory.class));

        CannedTokenStream cts = new CannedTokenStream(
            new Token("a", 1, 0, 1),
            new Token("b", 1, 2, 3),
            new Token("c", 0, 2, 3),
            new Token("b", 0, 2, 3),
            new Token("d", 1, 4, 5)
        );

        assertTokenStreamContents(tokenFilter.create(cts), new String[]{
            "a", "b", "c", "d"
        }, new int[]{
             1,   1,   0,   1
        });
    }

}
