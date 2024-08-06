package org.infinilabs.query;
import java.io.IOException;
import java.util.HashMap;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.roaringbitmap.RoaringBitmap;

public class FilterQuery 
	extends org.apache.lucene.search.Query {
	
	final private String field;	
	
	final private HashMap<String, RoaringBitmap> 
		docIds;
	
	public FilterQuery(
			String field0,
			HashMap<String, RoaringBitmap> docIds0) {
		
		field = field0;	
		docIds = docIds0;

		
		docIds.values().forEach(map -> map.add(DocIdSetIterator.NO_MORE_DOCS));
//		docIds.values().forEach(es::ensureAsc);
	}
	
	
	
	

	@Override
	public Weight createWeight(
			IndexSearcher searcher, 
			ScoreMode scoreMode, 
			float boost) throws IOException {

		return new ConstantScoreWeight(this, boost) {

			@Override
			public boolean isCacheable(LeafReaderContext ctx) {
//				return true;
				return false;
			}

			@SuppressWarnings("resource")
			@Override
			public Scorer scorer(LeafReaderContext context) throws IOException {
				
				LeafReader reader = 
						context.reader();
				while (reader instanceof FilterLeafReader) {
					reader = ((FilterLeafReader) reader).getDelegate();
				}
			
				String segmentName = 
						((SegmentReader) reader).getSegmentName();
				RoaringBitmap needGet1 =
						docIds.get(segmentName);
				
				if (needGet1 == null ||
					needGet1.isEmpty()) {
				
					//noop on this segment					
					return new ConstantScoreScorer(
						this, 
						score(), 
						scoreMode,
						DocIdSetIterator.empty());
				}
				

				DocIdSetIterator needGet3 = new IntArrayDocIdSet(
					needGet1.toArray(),
					needGet1.getCardinality() - 1)
						.iterator();				

				return new ConstantScoreScorer(
					this, 
					score(), 
					scoreMode,
					needGet3);
			}			
		};
	}

	@Override
	public void visit(QueryVisitor visitor) {
//		assert false;

//	    if (visitor.acceptField(field)) 
	        visitor.visitLeaf(this);	    
	}

	@Override
	public String toString(String field) {
		return "FilterQuery[" + this.field + "]";
	}

	  /**
	   * Override and implement query instance equivalence properly in a subclass. 
	   * This is required so that {@link QueryCache} works properly.
	   * 
	   * Typically a query will be equal to another only if it's an instance of 
	   * the same class and its document-filtering properties are identical that other
	   * instance. Utility methods are provided for certain repetitive code. 
	   * 
	   * @see #sameClassAs(Object)
	   * @see #classHash()
	   */
	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		
		if (this == obj)
			return true;
		if (obj.getClass() != FilterQuery.class)
			return false;
		FilterQuery obj2 = (FilterQuery) obj;
		
		return field.equals(obj2.field) &&
				docIds.equals(obj2.docIds);
	}

	  /**
	   * Utility method to check whether <code>other</code> is not null and is exactly 
	   * of the same class as this object's class.
	   * 
	   * When this method is used in an implementation of {@link #equals(Object)},
	   * consider using {@link #classHash()} in the implementation
	   * of {@link #hashCode} to differentiate different class
	   */	
	@Override
	public int hashCode() {
		return System.identityHashCode(this);
	}
}