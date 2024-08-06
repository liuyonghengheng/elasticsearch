package org.infinilabs.reload;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.infinilabs.FilterPlugin;
import org.infinilabs.FilterUtil;
import java.io.IOException;
import java.util.*;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.*;

public class RestIndicesReloadAction
	extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/{index}/_reload"),
            new Route(POST, "/{index}/_reload")));
    }

    @Override
    public String getName() {
        return "indices_reload_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

	@Override
    public RestChannelConsumer prepareRequest(
    		final RestRequest request,
    		final NodeClient client) throws IOException {


		if (FilterPlugin.getClient() == null) {
			FilterPlugin.setClient(client);
		}


        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
//        es.myAssert(indices.length == 1);
        if (indices.length != 1)
        	throw new IllegalArgumentException("Can only specify one index here.");

        if (indices[0].contains("*") ||
        	indices[0].contains("?"))
        	throw new IllegalArgumentException("Can only use normal index name here.");

    	IndicesReloadRequest r =
    			new IndicesReloadRequest(indices[0]);

        //localhost:9200/a,b/_reload //这样就是两个

//        es.show(Arrays.asList(indices));
//        r.indices(indices);

        //这里放执行transportAction的代码,,,

        return channel -> {

//            FlushRequest flushRequest =
//            		new FlushRequest(indices[0]);
//            flushRequest.indicesOptions(IndicesOptions.fromRequest(request, flushRequest.indicesOptions()));
//            flushRequest.force(request.paramAsBoolean("force", flushRequest.force()));
//            flushRequest.waitIfOngoing(request.paramAsBoolean("wait_if_ongoing", flushRequest.waitIfOngoing()));
//            client.admin().indices().flush(flushRequest, new RestToXContentListener<>(channel));

        	client.execute(
        		IndicesReloadAction.INSTANCE,
        		r,
        		FilterUtil.noopListener());

        	channel.sendResponse(new BytesRestResponse(
        		RestStatus.OK,
        		"Index[" + indices[0] + "] reload is okay..."));
        };

//        return channel -> client.admin().indices().stats(r, new RestToXContentListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
