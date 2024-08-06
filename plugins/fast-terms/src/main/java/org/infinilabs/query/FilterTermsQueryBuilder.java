package org.infinilabs.query;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.infinilabs.FilterPlugin;
import org.infinilabs.FilterUtil;
import org.roaringbitmap.RoaringBitmap;
import com.carrotsearch.hppc.IntObjectHashMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * A filter for a field based on several terms matching on any of them.
 */
public class FilterTermsQueryBuilder
	extends AbstractQueryBuilder<FilterTermsQueryBuilder> {

    public static final ParseField BITMAP_FIELD =
    		new ParseField("bitmap");
    public static final ParseField DOC_FIELD =
    		new ParseField("doc");

    public static final ParseField DOC_FIELD_Index =
    		new ParseField("index");
    public static final ParseField DOC_FIELD_Id =
    		new ParseField("id");
    public static final ParseField DOC_FIELD_Field =
    		new ParseField("field");
    public static final ParseField DOC_FIELD_TTL =
    		new ParseField("cache_ttl");

    public static final String NAME =
    		"fast_terms";

    private final String fieldName;
    private final RoaringBitmap values;

//    public FilterTermsQueryBuilder(String fieldName, int... values) {
//        this(fieldName, RoaringBitmap.bitmapOf(values));
//    }


    public FilterTermsQueryBuilder(String fieldName, RoaringBitmap values) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name cannot be null.");
        }
        if (values == null) {
            throw new IllegalArgumentException("No value specified for terms query");
        }
        this.fieldName = fieldName;
        this.values = values;
    }

    /**
     * Read from a stream.
     */
    public FilterTermsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
//        long begin = System.currentTimeMillis();
        this.values = RoaringBitmap.bitmapOf(in.readVIntArray());
//        FilterUtil.show((System.currentTimeMillis() - begin) + " bbbbbbbbbbbbb");
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
//        long begin = System.currentTimeMillis();
        out.writeVIntArray(values.toArray());
//        FilterUtil.show((System.currentTimeMillis() - begin) + " aaaaaaaaa");
    }

    public String fieldName() {
        return this.fieldName;
    }


    public List<Integer> values() {
        List<Integer> readableValues = new ArrayList<>();
        for (int value : values.toArray()) {
            readableValues.add(value);
        }
        return readableValues;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
            builder.field(fieldName, values());

        printBoostAndQueryName(builder);
        builder.endObject();
    }


	private enum ExecType {

    	Bitmap,
    	Doc
    }

    private static boolean badType2Arg(String s) {

    	return s == null || s.trim().isEmpty();
    }

    public static FilterTermsQueryBuilder fromXContent(
    		XContentParser parser) throws IOException {
    	//此解析只会在协调节点执行一次！（不会在分片节点执行）

    	//表示用哪种方式，初始是null
    	ExecType execType = null;

    	//方式1要解要的变量
    	String type1Bitmap = null;


    	//方式2要解析的变量                      //DSL里面的名字
    	String type2arg1 = null;     //index
    	String type2arg2 = null;     //id
    	String type2arg3 = null;     //field
    	String type2arg4 = null;     //cache_ttl


        String fieldName = null;

        //现在字段名不从DSL里面取，从yaml中取
        fieldName = FilterPlugin.FILTER_FIELD.get(FilterPlugin.settings);
//        assert false : fieldName;

        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        XContentParser.Token token;
        String curr = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {

            if (token == XContentParser.Token.FIELD_NAME) {
                curr = parser.currentName();
//                es.show(curr);

            }
            else
            	if (token == XContentParser.Token.START_ARRAY) {

            		//不应该有数组类型
            		if (true)
            			throw new ParsingException(parser.getTokenLocation(), "[" + FilterTermsQueryBuilder.NAME + "] query does not support multiple fields");

            		if (fieldName != null) {
            			throw new ParsingException(parser.getTokenLocation(), "[" + FilterTermsQueryBuilder.NAME + "] query does not support multiple fields");
            		}
            		fieldName = curr;
            	}
            	else
            		if (token == XContentParser.Token.START_OBJECT) {

		                if (FilterTermsQueryBuilder.DOC_FIELD.match(curr, parser.getDeprecationHandler())) {

		                    if (execType == ExecType.Bitmap) {
		                    	throw new ParsingException(parser.getTokenLocation(), "[" + FilterTermsQueryBuilder.NAME + "] can only specify one exec type of [bitmap, doc]");
		                    }

		                    execType = ExecType.Doc;

		                    //这里要另外一个循环来解析{}里面的参数
		                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {

		                        if (token == XContentParser.Token.FIELD_NAME) {
		                            curr = parser.currentName();
		                        }
		                        else {

		                            if (DOC_FIELD_Index.match(curr, parser.getDeprecationHandler())) {
		                            	type2arg1 = parser.text();
		                            } else if (DOC_FIELD_Id.match(curr, parser.getDeprecationHandler())) {
		                            	type2arg2 = parser.text();
		                            } else if (DOC_FIELD_Field.match(curr, parser.getDeprecationHandler())) {
		                            	type2arg3 = parser.text();
		                            } else if (DOC_FIELD_TTL.match(curr, parser.getDeprecationHandler())) {
		                            	type2arg4 = parser.text();

		                            } else {
		                                throw new ParsingException(parser.getTokenLocation(), "[fast_terms] query does not support [" + curr + "]");
		                            }
		                        }
		                    }
		                    //内层解析



//		                    throw new ParsingException(parser.getTokenLocation(),
//		                    	type2arg1 + ", " + type2arg2 + ", " + type2arg3 + ", " + type2arg4);

		                }
		                else
	                		throw new ParsingException(parser.getTokenLocation(), "[" + FilterTermsQueryBuilder.NAME + "] only allow 'doc' type here");
            		}
            		else
            			if (token.isValue()) {

            				//只能选其一
			                if (FilterTermsQueryBuilder.BITMAP_FIELD.match(curr, parser.getDeprecationHandler())) {
			                    if (execType == ExecType.Doc) {
			                    	throw new ParsingException(parser.getTokenLocation(), "[" + FilterTermsQueryBuilder.NAME + "] can only specify one exec type of [bitmap, doc]");
			                    }

			                    execType = ExecType.Bitmap;
			                    type1Bitmap = parser.text();

//			                    assert false : bitmapString;
			                }
			                else
			                	if (AbstractQueryBuilder.BOOST_FIELD.match(curr, parser.getDeprecationHandler())) {
			                		boost = parser.floatValue();
			                	}
			                	else
			                		if (AbstractQueryBuilder.NAME_FIELD.match(curr, parser.getDeprecationHandler())) {
			                			queryName = parser.text();
			                		}
			                		else
			                			throw new ParsingException(parser.getTokenLocation(), "[" + FilterTermsQueryBuilder.NAME + "] query does not support [" + curr + "]");
			            }
            			else
			                throw new ParsingException(parser.getTokenLocation(), "[" + FilterTermsQueryBuilder.NAME + "] unknown token [" + token + "] after [" + curr + "]");
        }


        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + FilterTermsQueryBuilder.NAME + "] query requires a field name, followed by array of terms or a document lookup specification");
        }

        if (execType == null) {
        	throw new ParsingException(parser.getTokenLocation(), "[" + FilterTermsQueryBuilder.NAME + "] must specify one exec type of [bitmap, doc]");
        }

        if (execType == ExecType.Bitmap) {
        	//现在就解析bitmap串，解析不了就报错

        	if (type1Bitmap.equals("test")) {
        		type1Bitmap = FilterUtil.testBitmapString;
        	}


        	RoaringBitmap map4BitmapType =
        			new RoaringBitmap();
        	try {
        		map4BitmapType.deserialize(
        			ByteBuffer.wrap(Base64.getDecoder().decode(type1Bitmap)));
        	}
        	catch (Exception e) {
        		throw new ParsingException(parser.getTokenLocation(), "[" + FilterTermsQueryBuilder.NAME + "] bitmap string '" + type1Bitmap + "' parse fail");
        	}


            return new FilterTermsQueryBuilder(fieldName, map4BitmapType)
            	.boost(boost)
            	.queryName(queryName);
        }




        //检查输入的4个参数
        //前3个必须指定，最后一个ttl可用缺省
        if (badType2Arg(type2arg1) ||
        	badType2Arg(type2arg2) ||
        	badType2Arg(type2arg3)) {

            throw new ParsingException(parser.getTokenLocation(),
            	"[" + FilterTermsQueryBuilder.NAME + "] must input valid args [index, id, field] when using 'doc' type");
        }

        //转换成秒（缺省1小时）
        long type2arg4Second = 3600;
        if (type2arg4 != null) {
        	try {
        		if (type2arg4.endsWith("m")) {

        			long type2arg4M =
        				Long.parseLong(type2arg4.substring(0, type2arg4.length() - 1));
        			type2arg4Second = 60 * type2arg4M;
        		}
        		else if (type2arg4.endsWith("h")) {

        			long type2arg4M =
        				Long.parseLong(type2arg4.substring(0, type2arg4.length() - 1));
        			type2arg4Second = 3600 * type2arg4M;
        		}
        		else {
                    throw new ParsingException(parser.getTokenLocation(),
                    	"[" + FilterTermsQueryBuilder.NAME + "] cache_ttl must be Xm or Xh format");
        		}

        		if (type2arg4Second <= 0 ||
        			type2arg4Second > 86400) {//1 day

                    throw new ParsingException(parser.getTokenLocation(),
                    	"[" + FilterTermsQueryBuilder.NAME + "] cache_ttl parse fail");
        		}
        	}
        	catch (Exception e) {
                throw new ParsingException(parser.getTokenLocation(),
                	"[" + FilterTermsQueryBuilder.NAME + "] cache_ttl parse fail");
        	}
        }





        final String cacheKey =
        	type2arg1 + ":" + type2arg2 + ":" + type2arg3;
        RoaringBitmap cacheInts =
        	FilterPlugin.cache4Doc.get(cacheKey);

        if (cacheInts != null) {
        	//有cache，从cache中取

        	return new FilterTermsQueryBuilder(fieldName, cacheInts)
            	.boost(boost)
            	.queryName(queryName);
        }


        //没有cache，只能查索引了
		if (FilterPlugin.getClient() == null) {
            throw new ParsingException(parser.getTokenLocation(),
                "[" + FilterTermsQueryBuilder.NAME + "] should exec reload in order to init the inner client");
		}

		GetRequest getRequest =	new GetRequest(type2arg1, type2arg2)
			.fetchSourceContext(new FetchSourceContext(
				true,
				new String[] {type2arg3}, //只取这一个
				null));

		final SetOnce<GetResponse> r1 =
			new SetOnce<>();

		FilterPlugin.getClient().get(
			getRequest,
			new ActionListener<GetResponse>() {

				@Override
				public void onResponse(GetResponse response) {
					r1.set(response);
				}

				@Override
				public void onFailure(Exception e) {
				}
			});


		int getCount = 0;
		while (r1.get() == null) {

			if (++getCount > 500) {
				break;//如果5秒还取不到，就算了
			}

			try {
				Thread.sleep(10);
			}
			catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}

		GetResponse r2 = r1.get();
		if (r2 == null ||
			!r2.isExists()) {

            throw new ParsingException(parser.getTokenLocation(),
            	"[" + FilterTermsQueryBuilder.NAME + "] get bitmap message fail, input correct [index, id, field]?");
		}


		String bitmap1 =
			(String) r2.getSource().get(type2arg3);

		RoaringBitmap map4DocType =
				new RoaringBitmap();

		try {
			map4DocType.deserialize(
				ByteBuffer.wrap(Base64.getDecoder().decode(bitmap1)));

			cacheInts = map4DocType;

			FilterPlugin.cache4Doc.put(
				cacheKey,
				cacheInts,
				type2arg4Second,  //上面定义好的
				TimeUnit.SECONDS);

			return new FilterTermsQueryBuilder(fieldName, cacheInts)
				.boost(boost)
				.queryName(queryName);
		}
		catch (Exception e) {
	        throw new ParsingException(parser.getTokenLocation(),
	        	"[" + FilterTermsQueryBuilder.NAME + "] get error for [" + type2arg1 + ", " + type2arg2 + ", " + type2arg3 + ", " + type2arg4 + "]");
		}
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }



	@Override
	protected Query doToQuery(QueryShardContext sec) throws IOException {

//    	es.star();

        if (values == null ||
        	values.isEmpty()) {
            throw new UnsupportedOperationException("query must be rewritten first");
        }

        //请求的id数量
        int valuesLen = values.getCardinality();
        //ES的限制
        int maxTermsCount =
        		sec.getIndexSettings().getMaxTermsCount();
        if (valuesLen > maxTermsCount) {
            throw new IllegalArgumentException("The number of terms ["  + valuesLen +  "] used in the Terms Query request has exceeded the allowed maximum of [" + maxTermsCount + "]. " + "This maximum can be set by changing the [" + IndexSettings.MAX_TERMS_COUNT_SETTING.getKey() + "] index level setting.");
        }

        MappedFieldType fieldType =
        		sec.fieldMapper(fieldName);
        if (fieldType == null)
            throw new IllegalStateException("Rewrite first");


        String cacheKey1 =
        	sec.getFullyQualifiedIndex().getName() + ":" +
            sec.getShardId();

        String cacheKey2 =
        	cacheKey1 + ":" +
        	values.hashCode();

        Query cacheForFetch =
        	FilterPlugin.fetchCaches.get(cacheKey2);

        if (cacheForFetch != null) {
        	//这发生在fetch阶须
        	return cacheForFetch;
        }


        //分片上存在的信息
        IntObjectHashMap<HashMap<String, RoaringBitmap>> shardCache =
        	FilterPlugin.shardCaches.get(cacheKey1);


        if (shardCache == null) {

        	FilterPlugin.logger.warn("Shard[" + cacheKey1 + "] has no bitmap message, you should reload it.");

        	//分片端没有bitmap，原路返回
        	ArrayList<Integer> ints =
            			new ArrayList<>();
            	for (int i : values.toArray())
            		ints.add(i);

            	return fieldType.termsQuery(ints, sec);
        }

            RoaringBitmap shardCache0 =
    	        	FilterPlugin.shardCaches0.get(cacheKey1);


            //应该返回这些Lucene文档列表
            //这个里面没有patent_id信息
            HashMap<String, RoaringBitmap> allDocIds =
            	new HashMap<>();

            long begin = System.currentTimeMillis();


            RoaringBitmap execBitmap = RoaringBitmap.and(
            	values,
            	shardCache0);

            for (int execPatentId : execBitmap.toArray()) {

            	shardCache.get(execPatentId).forEach((k, v) -> {

//            		if (!allDocIds.containsKey(k)) {
//            			allDocIds.put(k, new RoaringBitmap());
//            		}
//            		allDocIds.get(k).or(v);

            		allDocIds.computeIfAbsent(k, k2 -> new RoaringBitmap()).or(v);


            	});
            }

            FilterUtil.show((System.currentTimeMillis() - begin) + " 111111111111");

    	        if (allDocIds.isEmpty()) {
            		//此分片上没有要找的数据

//            		FilterPlugin.logger.warn("No match result for " + cacheKey1);

            		MatchNoDocsQuery cacheQuery =
            				new MatchNoDocsQuery(cacheKey1);

        			FilterPlugin.fetchCaches.put(
        					cacheKey2,
        					cacheQuery,
        					5, TimeUnit.MINUTES);
            		return cacheQuery;
    	        }

            	FilterQuery cacheQuery = new FilterQuery(
                	cacheKey1 + ":" + FilterPlugin.FILTER_FIELD.get(FilterPlugin.settings),
                	allDocIds);

                FilterPlugin.fetchCaches.put(
                	cacheKey2,
                	cacheQuery,
                	5, TimeUnit.MINUTES);

//            			FilterPlugin.logger.info("Request ids count:" + requestValues.size() + ", exec ids count:" + execValues.size() + " on shard[" + cacheKey + "]");
                return cacheQuery;

    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, values);
    }

    @Override
    protected boolean doEquals(FilterTermsQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
                Objects.equals(values, other.values);
    }

    @Override
    protected QueryBuilder doRewrite(
    		QueryRewriteContext queryRewriteContext) {

        if (values == null || values.isEmpty()) {
            return new MatchNoneQueryBuilder();
        }

		QueryShardContext context = queryRewriteContext.convertToShardContext();

        if (context != null) {
            MappedFieldType fieldType =
            		context.fieldMapper(this.fieldName);

            if (fieldType == null) {
                return new MatchNoneQueryBuilder();
            }
        }

        return this;
    }
}
