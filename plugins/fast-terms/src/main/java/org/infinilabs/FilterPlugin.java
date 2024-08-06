package org.infinilabs;
import java.util.*;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Counter;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.infinilabs.query.FilterTermsQueryBuilder;
import org.infinilabs.reload.IndicesReloadAction;
import org.infinilabs.reload.RestIndicesReloadAction;
import org.infinilabs.reload.TransportIndicesReloadAction;
import org.roaringbitmap.RoaringBitmap;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;
import net.jodah.expiringmap.ExpiringMap;

public class FilterPlugin
	extends Plugin
	implements ActionPlugin, SearchPlugin {

	private static NodeClient client;

	public static NodeClient getClient() {
		return client;
	}

	public static void setClient(NodeClient nc) {
		client = nc;
	}



	public static final Logger logger =
			LogManager.getLogger(FilterPlugin.class);
	static final Counter cnt =
			Counter.newCounter();

	public static Settings settings;


	public FilterPlugin(Settings settings) {
		FilterPlugin.settings = settings;
	}

	//要过滤的字段名
	final public static Setting<String> FILTER_FIELD = new Setting<>(
		"infini.filter.field",
		"id1",
		s -> {
            return check("infini.filter.field", s);
        },
		Setting.Property.NodeScope);

	//使用最小的cost值，缺省true
	final public static Setting<Boolean> FILTER_MIN_COST = Setting.boolSetting(
		"infini.filter.minCost",
		true,
		Setting.Property.NodeScope);

	//下面两个cache，k都是"index1:shard0"这种形多
	public static final Map<String, IntObjectHashMap<HashMap<String, RoaringBitmap>>>
		shardCaches = new HashMap<>();

	public static final Map<String, RoaringBitmap>
		shardCaches0 = new HashMap<>();


	//缓存了bmitmap串和对应的list<int>
	//一个search分为查询和取两个阶段
	//缓存下来可以减少取阶段的重复过滤，直接返回就可以了
	public static final ExpiringMap<String, Query>
		fetchCaches = ExpiringMap.builder()
			.variableExpiration()
			.build();


	//for "doc" type
	//key是DSL传入的"index:id:field"这个形式
	public static final ExpiringMap<String, RoaringBitmap>
		cache4Doc = ExpiringMap.builder()
			.variableExpiration()
			.build();


	private static String check(String k, String v) {
        if (v.isEmpty())
            throw new IllegalArgumentException("[" + k + "] must not be empty");

        if (v.contains(":"))
            throw new IllegalArgumentException("[" + k + "] must not contain ':'");

        return v;
	}

    @Override
    public List<Setting<?>> getSettings() {

        return Arrays.asList(FILTER_FIELD, FILTER_MIN_COST);
    }


    @Override
    public List<QuerySpec<?>> getQueries() {

        return Arrays.asList(new QuerySpec<>(
        	FilterTermsQueryBuilder.NAME,
        	FilterTermsQueryBuilder::new,
        	FilterTermsQueryBuilder::fromXContent));
    }


    @Override
    public List<RestHandler> getRestHandlers(
    		Settings settings,
    		RestController restController,
    		ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {

    	return Arrays.asList(new RestIndicesReloadAction());
    }

    @Override
    public List<ActionHandler<
    		? extends ActionRequest,
    		? extends ActionResponse>> getActions() {

        return Arrays.asList(new ActionHandler<>(
        		IndicesReloadAction.INSTANCE,
        		TransportIndicesReloadAction.class));
    }
}
