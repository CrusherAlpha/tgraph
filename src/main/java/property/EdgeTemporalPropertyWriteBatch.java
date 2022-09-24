package property;

import common.Codec;
import kvstore.KVEngine;
import kvstore.WriteBatch;

public class EdgeTemporalPropertyWriteBatch implements AutoCloseable {

    private final WriteBatch wb;

    // We need store to implement remove Prefix.
    private final KVEngine store;

    public EdgeTemporalPropertyWriteBatch(WriteBatch wb, KVEngine store) {
        this.wb = wb;
        this.store = store;
    }

    boolean put(EdgeTemporalPropertyKey key, Object value) {
        return wb.put(key.toBytes(), Codec.encodeValue(value));
    }

    boolean remove(EdgeTemporalPropertyKey key) {
        return wb.remove(key.toBytes());
    }

    boolean removeRange(EdgeTemporalPropertyKey start, EdgeTemporalPropertyKey end) {
        return wb.removeRange(start.toBytes(), end.toBytes());
    }

    boolean removePrefix(EdgeTemporalPropertyKeyPrefix prefix) {
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
