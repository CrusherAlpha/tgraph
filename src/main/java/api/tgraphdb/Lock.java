package api.tgraphdb;


/**
 * An acquired lock on an entity for a transaction, acquired from
 * Transaction#acquireWriteLock(Entity) or Transaction#acquireReadLock(Entity)
 * this lock can be released manually using release(). If not released
 * manually it will be automatically released when the transaction owning
 * it finishes.
 */
public interface Lock extends AutoCloseable {
    /**
     * Releases this lock before the transaction finishes. It is an optional
     * operation and if not called, this lock will be released when the owning
     * transaction finishes.
     *
     * @throws IllegalStateException if this lock has already been released.
     */
    void release();

    /**
     * Release this lock as described by release() method.
     *
     * @throws IllegalStateException if this lock has already been released.
     */
    @Override
    void close();
}
