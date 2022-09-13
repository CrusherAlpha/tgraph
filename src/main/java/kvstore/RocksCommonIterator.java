package kvstore;

import org.rocksdb.RocksIterator;

public class RocksCommonIterator implements KVIterator {
    private final RocksIterator iter;
    RocksCommonIterator(RocksIterator iter) {
        this.iter = iter;
    }
    @Override
    public boolean valid() {
        return iter != null && iter.isValid();
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
