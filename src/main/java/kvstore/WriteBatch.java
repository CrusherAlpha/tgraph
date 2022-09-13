package kvstore;

// wrapper of batch write
public interface WriteBatch {
    /**
     *
     * @param key Key to put
     * @param value Value to put
     * @return succeed or not.
     */
    boolean put(byte[] key, byte[] value);

    /**
     *
     * @param key Key to remove
     * @return succeed or not.
     */
    boolean remove(byte[] key);

    /**
     * Remove the keys between [start, end).
     * @param start key left range
     * @param end key right range
     * @return succeed or not.
     */
    boolean removeRange(byte[] start, byte[] end);

    void close();
}
