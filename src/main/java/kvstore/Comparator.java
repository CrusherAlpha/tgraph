package kvstore;

import org.rocksdb.AbstractComparator;

import java.nio.ByteBuffer;

public class Comparator {
    private final AbstractComparator rocksComparator;

    public Comparator(AbstractComparator rocksComparator) {
        this.rocksComparator = rocksComparator;
    }

    public int compare(byte[] start, byte[] end) {
        return rocksComparator.compare(ByteBuffer.wrap(start), ByteBuffer.wrap(end));
    }

    public AbstractComparator getRocksComparator() {
        return rocksComparator;
    }
}
