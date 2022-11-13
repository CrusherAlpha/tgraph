package cn.edu.buaa.act.tgraph.kvstore;

import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;

import static cn.edu.buaa.act.tgraph.common.Bytes.memcmp;

public class RocksRangeIterator implements KVIterator {
    private final byte[] end;
    private final RocksIterator iter;
    private final Comparator comparator;

    public RocksRangeIterator(byte[] end, RocksIterator iter, Comparator comparator) {
        this.end = end;
        this.iter = iter;
        this.comparator = comparator;
    }

    private int compare(byte[] start, byte[] end) {
        if (comparator != null) {
            return comparator.compare(start, end);
        }
        // if comparator is null, we use bytewise comparator
        int minLen = Math.min(start.length, end.length);
        int r = memcmp(ByteBuffer.wrap(start), ByteBuffer.wrap(end), 0, minLen);
        if (r == 0) {
            if (start.length < end.length) {
                r = -1;
            } else if (start.length > end.length) {
                r = 1;
            }
        }
        return r;
    }

    @Override
    public boolean valid() {
        return iter != null && iter.isValid() && compare(iter.key(), end) < 0;
    }

    @Override
    public void next() {
        iter.next();
    }

    @Override
    public void prev() {
        iter.prev();
    }

    @Override
    public byte[] key() {
        return iter.key();
    }

    @Override
    public byte[] value() {
        return iter.value();
    }

    @Override
    public void close() {
        iter.close();
    }
}
