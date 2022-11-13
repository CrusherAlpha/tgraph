package cn.edu.buaa.act.tgraph.kvstore;

import cn.edu.buaa.act.tgraph.common.Pair;

import java.util.List;

// KVEngine is used for log store and graph temporal property
// store, currently we have rocksdb implementation.
// TODO(crusher): remove unused return.
public interface KVEngine {

    void stop();

    /**
     *
     * @return the directory path of kv engine
     */
    String getRoot();

    /**
     *
     * @return a WriteBatch object to do batch operation.
     */
    WriteBatch startBatchWrite();


    /**
     *
     * @param batch WriteBatch object.
     * @param disableWAL Whether wal is disabled, only used in rocksdb.
     * @param sync Whether you need to sync when writing, only used in rocksdb.
     * @param wait Whether you wait until write result, rocksdb would return incomplete
     *             if wait is false in certain scenario
     * @return Status of this operation
     */
    boolean commitBatchWrite(WriteBatch batch, boolean disableWAL, boolean sync, boolean wait);

    /**
     * Get the snapshot from kv engine
     * @return current snapshot
     */
    Object getSnapshot();

    /**
     *
     * @param snapshot Snapshot to release
     */
    void releaseSnapshot(final Object snapshot);

    /**
     *
     * @param key Key to read
     * @param snapshot read from this snapshot
     * @return Value to this Key or null
     */
    byte[] get(byte[] key, Object snapshot);

    List<byte[]> multiGet(List<byte[]> keys);

    /**
     * Get the previous key less or equal key.
     * We need this because when we want to know the value in 9:00 am,
     * the effect is caused by operations less or equal 9:00 am.
     * @param key Key
     * @param snapshot read from this snapshot
     * @return Pair: first is prev_key, second is prev_value
     */
    Pair<byte[], byte[]> getForPrev(byte[] key, Object snapshot);

    List<Pair<byte[], byte[]>> multiGetForPrev(List<byte[]> keys);

    /**
     * Get all results in range [start, end).
     * @param start Start key, inclusive.
     * @param end End key, exclusive.
     * @return Iterator in range [start, end).
     */
    KVIterator range(byte[] start, byte[] end);

    /**
     * Get all results which is the max key <= key in range [start, end).
     * Eg: <k1, v1>, <k4, v4>,  <k9, v9> in db.
     * when user pass [2, 5), v1 and v4 will be returned.
     * @param start Start key, inclusive.
     * @param end End key, exclusive.
     * @return Iterator in range [start, end).
     */
    KVIterator rangePrev(byte[] start, byte[] end);

    /**
     * Get all results with 'prefix' as prefix.
     * @param prefix The prefix of keys to iterate.
     * @param snapshot Snapshot from kv engine, null means no snapshot.
     * @return Iterator of keys starts with 'prefix'.
     */
    KVIterator prefix(byte[] prefix, Object snapshot);

    /**
     * Get all results with 'prefix' as prefix starting from 'start'.
     * @param start Start key, inclusive.
     * @param prefix The prefix of keys to iterate.
     * @return Iterator of keys starts with 'prefix' beginning from 'start'.
     */
    KVIterator rangeWithPrefix(byte[] start, byte[] prefix);

    /**
     * Get all results with 'prefix' as prefix starting from the key which is the max key <= 'start'.
     * @param start Start key, inclusive.
     * @param prefix The prefix of keys to iterate.
     * @return Iterator of keys starts with 'prefix' beginning from the key which is the max key <= 'start'.
     */
    KVIterator rangePrevWithPrefix(byte[] start, byte[] prefix);

    /**
     * Put a single key/value pair.
     * @param key Key
     * @param value Value
     * @return Status of this operation
     */
    boolean put(byte[] key, byte[] value);

    /**
     *
     * @param keyValues key/value pairs
     * @return Status of this operation
     */
    boolean multiPut(List<Pair<byte[], byte[]>> keyValues);

    /**
     *  Remove a single key.
     * @param key Key
     * @return Status of this operation
     */
    boolean remove(byte[] key);

    /**
     *
     * @param keys Keys
     * @return Status of this operation
     */
    boolean multiRemove(List<byte[]> keys);

    /**
     * Remove key in range [start, end).
     * @param start Start key, inclusive.
     * @param end End key, exclusive.
     * @return Status of this operation
     */
    boolean removeRange(byte[] start, byte[] end);

    /**
     * Flush data in memory into disk.
     * @return Status of this operation
     */
    boolean flush();

    // NOTE!: only used in meta db.
    List<Pair<byte[], byte[]>> scan();

    /**
     * Drop this store.
     */
    void drop();
}
