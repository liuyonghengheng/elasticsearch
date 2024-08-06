


package org.elasticsearch.sql.legacy.metrics;

import java.util.concurrent.atomic.LongAdder;

public class BasicCounter implements Counter<Long> {

    private LongAdder count = new LongAdder();

    @Override
    public void increment() {
        count.increment();
    }

    @Override
    public void add(long n) {
        count.add(n);
    }

    @Override
    public Long getValue() {
        return count.longValue();
    }

    @Override
    public void reset() {
        count.reset();
    }
}
