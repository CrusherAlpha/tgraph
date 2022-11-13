package cn.edu.buaa.act.tgraph.property;

import cn.edu.buaa.act.tgraph.common.Codec;
import cn.edu.buaa.act.tgraph.kvstore.KVEngine;
import cn.edu.buaa.act.tgraph.kvstore.WriteBatch;

public class EdgeTemporalPropertyWriteBatch implements AutoCloseable {

    private final WriteBatch wb;

    // We need store to implement remove Prefix.
    private final KVEngine store;

    public EdgeTemporalPropertyWriteBatch(WriteBatch wb, KVEngine store) {
        this.wb = wb;
        this.store = store;
    }

    public boolean put(EdgeTemporalPropertyKey key, Object value) {
        return wb.put(key.toBytes(), Codec.encodeValue(value));
    }

    public boolean remove(EdgeTemporalPropertyKey key) {
        return wb.remove(key.toBytes());
    }

    public boolean removeRange(EdgeTemporalPropertyKey start, EdgeTemporalPropertyKey end) {
        return wb.removeRange(start.toBytes(), end.toBytes());
    }

    public boolean removePrefix(EdgeTemporalPropertyKeyPrefix prefix) {
        try (var iter = store.prefix(prefix.toBytes(), null)) {
            while (iter.valid()) {
                wb.remove(iter.key());
                iter.next();
            }
        }
        return true;
    }

    public WriteBatch getWb() {
        return wb;
    }

    @Override
    public void close() {
        wb.close();
    }
}
