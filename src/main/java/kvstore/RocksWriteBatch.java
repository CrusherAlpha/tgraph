package kvstore;

import org.rocksdb.RocksDBException;

public class RocksWriteBatch implements WriteBatch {
    private final org.rocksdb.WriteBatch wb;

    RocksWriteBatch() {
        this.wb = new org.rocksdb.WriteBatch(RocksEngineConfig.rocksdb_batch_size);
    }
    @Override
    public boolean put(byte[] key, byte[] value) {
        try {
            wb.put(key, value);
        } catch (RocksDBException e) {
            return false;
        }
        return true;
    }

    @Override
    public boolean remove(byte[] key) {
        try {
            wb.delete(key);
        } catch (RocksDBException e) {
            return false;
        }
        return true;
    }

    @Override
    public boolean removeRange(byte[] start, byte[] end) {
        try {
            wb.deleteRange(start, end);
        } catch (RocksDBException e) {
            return false;
        }
        return true;
    }

    @Override
    public void close() {
        wb.close();
    }

    // NOTE!: caller should guarantee call this before close
    public org.rocksdb.WriteBatch getWb() {
        return wb;
    }
}
