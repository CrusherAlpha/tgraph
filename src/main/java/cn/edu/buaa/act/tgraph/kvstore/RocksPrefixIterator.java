package cn.edu.buaa.act.tgraph.kvstore;

import cn.edu.buaa.act.tgraph.common.Bytes;
import org.rocksdb.RocksIterator;

public class RocksPrefixIterator implements KVIterator {
    private final byte[] prefix;
    private final RocksIterator iter;


    public RocksPrefixIterator(byte[] prefix, RocksIterator iter) {
        this.prefix = prefix;
        this.iter = iter;
    }

    @Override
    public boolean valid() {
        return iter != null && iter.isValid() && Bytes.startsWith(iter.key(), prefix);
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
