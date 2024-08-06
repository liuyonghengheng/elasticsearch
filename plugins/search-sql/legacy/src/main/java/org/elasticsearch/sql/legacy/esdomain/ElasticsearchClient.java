


package org.elasticsearch.sql.legacy.esdomain;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.sql.legacy.query.join.BackOffRetryStrategy;

public class ElasticsearchClient {

    private static final Logger LOG = LogManager.getLogger();
    private static final int[] retryIntervals = new int[]{4, 12, 20, 20};
    private final Client client;

    public ElasticsearchClient(Client client) {
        this.client = client;
    }

    public MultiSearchResponse.Item[] multiSearch(MultiSearchRequest multiSearchRequest) {
        MultiSearchResponse.Item[] responses = new MultiSearchResponse.Item[multiSearchRequest.requests().size()];
        multiSearchRetry(responses, multiSearchRequest,
                IntStream.range(0, multiSearchRequest.requests().size()).boxed().collect(Collectors.toList()), 0);

        return responses;
    }

    private void multiSearchRetry(MultiSearchResponse.Item[] responses, MultiSearchRequest multiSearchRequest,
                                  List<Integer> indices, int retry) {
        MultiSearchRequest multiSearchRequestRetry = new MultiSearchRequest();
        for (int i : indices) {
            multiSearchRequestRetry.add(multiSearchRequest.requests().get(i));
        }
        MultiSearchResponse.Item[] res = client.multiSearch(multiSearchRequestRetry).actionGet().getResponses();
        List<Integer> indicesFailure = new ArrayList<>();
        //Could get EsRejectedExecutionException and ElasticsearchException as getCause
        for (int i = 0; i < res.length; i++) {
            if (res[i].isFailure()) {
                indicesFailure.add(indices.get(i));
                if (retry == 3) {
                    responses[indices.get(i)] = res[i];
                }
            } else {
                responses[indices.get(i)] = res[i];
            }
        }
        if (!indicesFailure.isEmpty()) {
            LOG.info("Elasticsearch multisearch has failures on retry {}", retry);
            if (retry < 3) {
                BackOffRetryStrategy.backOffSleep(retryIntervals[retry]);
                multiSearchRetry(responses, multiSearchRequest, indicesFailure, retry + 1);
            }
        }
    }
}
