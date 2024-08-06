


package org.elasticsearch.sql.legacy.metrics;

public interface Counter<T> {

    void increment();

    void add(long n);

    T getValue();

    void reset();
}
