package org.infinilabs.reload;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.infinilabs.FilterPlugin;
import org.roaringbitmap.RoaringBitmap;
import com.carrotsearch.hppc.IntObjectHashMap;
import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportIndicesReloadAction
	extends TransportBroadcastByNodeAction<
		IndicesReloadRequest, //索引请求
		IndicesReloadResponse, //索引回应
		ShardStats> { //分片操作结果

    private final IndicesService indicesService;

    private final Settings settings;

    @Inject
    public TransportIndicesReloadAction(
    		ClusterService clusterService,
    		TransportService transportService,
    		IndicesService indicesService,
    		ActionFilters actionFilters,
    		IndexNameExpressionResolver indexNameExpressionResolver) {

        super(IndicesReloadAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                IndicesReloadRequest::new, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;

        //从这个里面取一下下
        this.settings = clusterService.getSettings();
//        this.nodeName = transportService.getLocalNode().getName();
//        es.show("??????????????????");
    }

    @Override
    protected ShardsIterator shards(
    		ClusterState clusterState,
    		IndicesReloadRequest request,
    		String[] concreteIndices) {

        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(
    		ClusterState cs,
    		IndicesReloadRequest request) {

        return cs.blocks().globalBlockedException(
        	ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(
    		ClusterState state,
    		IndicesReloadRequest request,
    		String[] concreteIndices) {

        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardStats readShardResult(StreamInput in) throws IOException {
        return new ShardStats(in);
    }

    @Override
    protected IndicesReloadResponse newResponse(
    		IndicesReloadRequest request,
    		int totalShards,
    		int successfulShards,
    		int failedShards,
    		List<ShardStats> responses,
    		List<DefaultShardOperationFailedException> shardFailures,
    		ClusterState clusterState) {

        return new IndicesReloadResponse(
        	responses.toArray(new ShardStats[responses.size()]),
        	totalShards,
        	successfulShards,
        	failedShards,
            shardFailures);
    }

    @Override
    protected IndicesReloadRequest readRequestFrom(
    		StreamInput in) throws IOException {

        return new IndicesReloadRequest(in);
    }

	@Override
	protected void doExecute(Task task, IndicesReloadRequest request, ActionListener<IndicesReloadResponse> listener) {
//		request.
//    	listener.onResponse(new ShardStats(
//				indexShard.routingEntry()));
    	super.doExecute(task, request, listener);
	}

	@Override
	protected ShardStats shardOperation(IndicesReloadRequest request, ShardRouting sr) throws IOException {

    	//数据有变化才需要_reload，
    	//所以要清一下缓存
    	if (!FilterPlugin.fetchCaches.isEmpty()) {
    		FilterPlugin.fetchCaches.clear();
    	}

    	//1 索引
        IndexService indexService =
        		indicesService.indexServiceSafe(sr.shardId().getIndex());
        //2 分片
        IndexShard indexShard =
        		indexService.getShard(sr.shardId().id());

        // if we don't have the routing entry yet,
        // we need it stats wise, we treat it as if the shard is not ready yet
        if (indexShard.routingEntry() == null) {
            throw new ShardNotFoundException(indexShard.shardId());
        }


        String idxName =
        		sr.getIndexName();
        String fldName =
        		FilterPlugin.FILTER_FIELD.get(settings);

//        es.show("vvvvvvvvvvvvvvvvvvv " + idxName);

        //用来判断本分片是不是有这个patent_id，也许能提高效率
        RoaringBitmap shardCache0 =
        		new RoaringBitmap();

        IntObjectHashMap<HashMap<String, RoaringBitmap>> shardCache =
        		new IntObjectHashMap<>();


        String keyName =
        		idxName + ":" + sr.shardId().id();

		System.out.println(keyName);

        org.apache.lucene.store.Directory dir =
        		indexShard.store().directory();

        DirectoryReader dr = null;
		try {
			dr = DirectoryReader.open(dir);
		}
		catch (IOException e1) {
			e1.printStackTrace();
		}

        List<LeafReaderContext> leafs =
        		dr.leaves();

        AtomicInteger count =
        		new AtomicInteger();


        FilterPlugin.shardCaches0.put(keyName, null);
        FilterPlugin.shardCaches.put(keyName, null);

        leafs.stream().map(leaf -> leaf.reader()).forEach((LeafReader reader) -> {

        	//不同的SegmentReader里面会有相同的Lucene文档号，要区分
        	//"_0", "_1", "_2" 这种形式
        	String segName =
        		((SegmentReader) reader).getSegmentName();

        	SortedNumericDocValues dvs;

        	try {
        		dvs = reader.getSortedNumericDocValues(fldName);

        		for (int doc = dvs.nextDoc();
        			doc != DocIdSetIterator.NO_MORE_DOCS;
        			doc = dvs.nextDoc()) {

        			assert doc == dvs.docID();

        			int patentId =
        				(int) dvs.nextValue();

        			if (!shardCache.containsKey(patentId)) {
        				shardCache.put(patentId, new HashMap<>());
        			}

        			shardCache0.add(patentId);

        			//这个patentId的信息
        			HashMap<String, RoaringBitmap> pf1 =
        					shardCache.get(patentId);

        			if (!pf1.containsKey(segName)) {
        				pf1.put(segName, new RoaringBitmap());
        			}

        			System.out.println(segName + ", " + doc);
        			pf1.get(segName).add(doc);



        			count.incrementAndGet();
        		}
        	}
        	catch (IOException e) {
        		e.printStackTrace();
        	}
        });


        FilterPlugin.logger.info("Reload bitmap " + count.get() + " count for [" + keyName + "][" +
        		(sr.primary() ? "P" : "R") + "]");

        FilterPlugin.shardCaches0.put(keyName, shardCache0);
        FilterPlugin.shardCaches.put(keyName, shardCache);


//        es.show(keyName + " ---> " + shardCache);
        try {
        	if (dr != null)
        		dr.close();
		}
        catch (IOException e) {
			e.printStackTrace();
		}

//        al.onResponse(new ShardStats(
//                indexShard.routingEntry()));
		return null;
    }
}
