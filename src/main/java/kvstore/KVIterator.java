package kvstore;

public interface KVIterator extends AutoCloseable {
    /**
     *
     * @return whether iterator has more key/value.
     */
    boolean valid();

    /**
     * Move to next key/value, undefined behavior when valid is false.
     */
    void next();

    /**
     * Move to previous key/value.
     */
    void prev();

    /**
     *
     * @return Key
     */
    byte[] key();

    /**
     *
     * @return Value
     */
    byte[] value();

    @Override
    void close();
}
