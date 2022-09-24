package property;

import common.Codec;
import kvstore.KVEngine;
import kvstore.WriteBatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class VertexTemporalPropertyWriteBatch implements AutoCloseable {

    private final WriteBatch wb;

    // We need store to implement removePrefix.
    private final KVEngine store;

    private static final Log log = LogFactory.getLog(VertexTemporalPropertyWriteBatch.class);

    public VertexTemporalPropertyWriteBatch(WriteBatch wb, KVEngine store) {
        this.wb = wb;
        this.store = store;
    }

    boolean put(VertexTemporalPropertyKey key, Object value) {
        return wb.put(key.toBytes(), Codec.encodeValue(value));
    }

    boolean remove(VertexTemporalPropertyKey key) {
        return wb.remove(key.toBytes());
    }

    boolean removeRange(VertexTemporalPropertyKey start, VertexTemporalPropertyKey end) {
        return wb.removeRange(start.toBytes(), end.toBytes());
    }

    boolean removePrefix(VertexTemporalPropertyKeyPrefix prefix) {
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
