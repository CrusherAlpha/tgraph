package kvstore;

import common.BytewiseComparator;
import org.rocksdb.RocksIterator;

public class RocksRangeIterator implements KVIterator {
    private final byte[] end;
    private final RocksIterator iter;

    public RocksRangeIterator(byte[] start, byte[] end, RocksIterator iter) {
        this.end = end;
        this.iter = iter;
        if (iter != null) {
            iter.seek(start);
        }
    }

    @Override
    public boolean valid() {
        return iter != null && iter.isValid() && BytewiseComparator.INSTANCE.getInstance().compare(iter.key(), end) < 0;
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
