package common;

import org.rocksdb.ComparatorOptions;

import java.nio.ByteBuffer;

// thread safe bytewise comparator singleton.
// a simple wrapper of rocksdb bytewise comparator.
public enum BytewiseComparator {
    INSTANCE(new org.rocksdb.util.BytewiseComparator(new ComparatorOptions()));

    private final org.rocksdb.util.BytewiseComparator comparator;

    BytewiseComparator(org.rocksdb.util.BytewiseComparator comparator) {
        this.comparator = comparator;
    }

    public BytewiseComparator getInstance() {
        return INSTANCE;
    }

    public int compare(byte[] left, byte[] right) {
        return comparator.compare(ByteBuffer.wrap(left), ByteBuffer.wrap(right));
    }


}
