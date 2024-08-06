package ltd.infini.search.filters;

import net.openhft.hashing.LongHashFunction;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.FilterScript.LeafFactory;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class TermsFilterPlugin extends Plugin implements ScriptPlugin {

	@Override
	public ScriptEngine getScriptEngine(
			Settings settings,
			Collection<ScriptContext<?>> contexts
			) {
		return new InfiniCollapseFilterEngine();
	}

	private static class InfiniCollapseFilterEngine implements ScriptEngine {
		@Override
		public String getType() {
			return "infini_ext";
		}

		@Override
		public <T> T compile(
				String scriptName,
				String scriptSource,
				ScriptContext<T> context,
				Map<String, String> params
				) {

			if (context.equals(FilterScript.CONTEXT) == false) {
				throw new IllegalArgumentException(getType()
						+ " scripts cannot be used for context ["
						+ context.name + "]");
			}

			if ("filter_by_terms".equals(scriptSource)) {
				FilterScript.Factory factory = new FastFilterFactory();
				return context.factoryClazz.cast(factory);
			}
			throw new IllegalArgumentException("Unknown script name "
					+ scriptSource);
		}

		@Override
		public void close() {
		}

		@Override
		public Set<ScriptContext<?>> getSupportedContexts() {
			return this.getSupportedContexts();
		}

		private static class FastFilterFactory implements FilterScript.Factory,
		ScriptFactory {
			@Override
			public boolean isResultDeterministic() {
				return true;
			}


			@Override
			public LeafFactory newFactory(
					Map<String, Object> params,
					SearchLookup lookup
					) {

				Roaring64Bitmap rBitmap = new Roaring64Bitmap();
				if (params.containsKey("hash")){
					String hashStr = params.get("hash").toString();
					final byte[] decodedTerms = Base64.getDecoder().decode(hashStr);
					final ByteBuffer buffer = ByteBuffer.wrap(decodedTerms);
					try {
						rBitmap.deserialize(buffer);
					}
					catch (IOException e) {
						System.out.println(e);
						// Do something here
					}

					//TODO Take the intersection with local bitmap

				}else if (params.containsKey("terms")){

					//check type, term is string or long
					String fieldType = "long";
					if (params.containsKey("type")){
						fieldType = params.get("type").toString();
					}

					ArrayList terms = (ArrayList) params.get("terms");
					if (terms.size()>0){
						for(int i=0;i<terms.size();i++){
							String v = terms.get(i).toString();
							if (fieldType.equals("long")){
								long x = Long.parseLong(v);
								rBitmap.add(x);
							}else{
								Long l = LongHashFunction.xx().hashChars(v);
								rBitmap.add(l);
							}
						}
					}
				}
				return new FastFilterLeafFactory(params, lookup, rBitmap);
		}

		private static class FastFilterLeafFactory implements LeafFactory {
			private final Map<String, Object> params;
			private final SearchLookup lookup;
			private final String fieldName;
			private final String opType;
			private final String fieldType;
			private final Roaring64Bitmap rBitmap;

			private FastFilterLeafFactory(Map<String, Object> params, SearchLookup lookup, Roaring64Bitmap rBitmap) {
				if (!params.containsKey("field")) {
					throw new IllegalArgumentException(
							"Missing parameter [field]");
				}

				if ((!params.containsKey("terms")) && (!params.containsKey("hash"))) {
					throw new IllegalArgumentException(
							"Missing parameter [terms] or [hash]");
				}

				this.params = params;
				this.lookup = lookup;
				this.rBitmap = rBitmap;
				opType = params.get("operation").toString();
				fieldName = params.get("field").toString();
				if (params.containsKey("type")){
					fieldType = params.get("type").toString();
				}else{
					fieldType = "long";
				}
			}

			@Override
			public FilterScript newInstance(LeafReaderContext context) throws IOException {
				return new FilterScript(params, lookup, context) {

					@Override
					public boolean execute() {
						try {
							Map<String, ScriptDocValues<?>> docs = getDoc();
							long hash;
							switch (fieldType){
								case "double":
									ScriptDocValues.Doubles fieldNameValue = (ScriptDocValues.Doubles) getDoc().get(fieldName);
									hash = (long) fieldNameValue.getValue();
									break;
								case "long":
									 ScriptDocValues.Longs longFieldNameValue =
										(ScriptDocValues.Longs)docs.get(fieldName);
									 hash =  longFieldNameValue.getValue();
									break;
								default:
									final ScriptDocValues.Strings fieldNameValue1 =
											(ScriptDocValues.Strings)docs.get(fieldName);
									hash=LongHashFunction.xx().hashChars(fieldNameValue1.getValue());
									break;
							}
							if (opType.equals("exclude") && rBitmap.contains(hash)) {
								return false;
							}else if (opType.equals("include") && !rBitmap.contains(hash)) {
								return false;
							}
							return true;

						} catch (Exception exception) {
							throw exception;
						}
					}
				};
			}
		}
	}
}
}
