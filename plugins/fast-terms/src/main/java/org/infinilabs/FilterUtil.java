package org.infinilabs;
import java.util.Random;
import org.elasticsearch.action.ActionListener;
import com.carrotsearch.hppc.IntArrayList;

public enum FilterUtil {
	;

	public static String testBitmapString =
		"==";


	public static void ensureAsc(IntArrayList list) {
		if (list == null ||
			list.isEmpty() ||
			list.size() == 1)
			return;

		int[] data = list.toArray();
		int dataLen = data.length;

		//比如有5个元素
		//这里的idx是0, 1, 2, 3（4不需要）
		for (int idx = 0; idx <= dataLen - 2; idx++) {

			assert data[idx] != data[idx + 1] : data[idx];

			if (data[idx] > data[idx + 1])
				throw new RuntimeException(data[idx] + " > " + data[idx + 1]);

//			assert data[idx] < data[idx + 1] : data[idx] + " vs " + data[idx + 1];
		}
	}



    @SuppressWarnings("rawtypes")
	private static ActionListener NOOP = new ActionListener() {

		@Override
		public void onResponse(Object response) {
		}

		@Override
		public void onFailure(Exception e) {
			e.printStackTrace();
		}
    };


    @SuppressWarnings("unchecked")
	public static <T> ActionListener<T> noopListener() {
        return (ActionListener<T>) NOOP;
    }

	public static void show(Object o) {
		System.out.println(FilterPlugin.cnt.addAndGet(1) + " -----> " + o);
	}

	final public static Random rand =
		new Random();
}
