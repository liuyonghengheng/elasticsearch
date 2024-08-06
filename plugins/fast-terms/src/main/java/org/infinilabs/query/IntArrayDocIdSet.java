package org.infinilabs.query;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.search.DocIdSetIterator;
import org.infinilabs.FilterPlugin;

public class IntArrayDocIdSet {

	private final int[] docs;
	private final int length;

	public IntArrayDocIdSet(
			int[] docs,   //最后一个元素是DocIdSetIterator.NO_MORE_DOCS
			int length) { //有效的元数个数，docs.length - 1		
		
		if (docs[length] != DocIdSetIterator.NO_MORE_DOCS) {
			throw new IllegalArgumentException();
		}
		this.docs = docs;
		this.length = length;
	}

	public DocIdSetIterator iterator() throws IOException {
		return new MyDISI(docs, length);
	}
	
	
	

	static class MyDISI
		extends DocIdSetIterator {

		private final int[] docs;
		private final int length;
		private int i = 0;
		private int doc = -1;

		MyDISI(int[] docs, int length) {
			this.docs = docs;
			this.length = length;
		}

		@Override
		public int docID() {
			return doc;
		}

		@Override
		public int nextDoc() throws IOException {
			return doc = docs[i++];
		}

		@Override
		public int advance(int target) throws IOException {
//			assert false : target;
			
			int bound = 1;

			while (i + bound < length && docs[i + bound] < target) {
				bound *= 2;
			}
			
			i = Arrays.binarySearch(docs, i + bound / 2, Math.min(i + bound + 1, length), target);
			if (i < 0) {
				i = -1 - i;
			}
			
			return doc = docs[i++];
		}

		@Override
		public long cost() {
			
			//最小的cost
			if (FilterPlugin.FILTER_MIN_COST.get(FilterPlugin.settings)) {
				return Long.MIN_VALUE;
			}			

			return length;
		}
	}
}