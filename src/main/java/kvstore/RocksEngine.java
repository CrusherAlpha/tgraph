package kvstore;

import com.google.common.base.Preconditions;
import common.Codec;
import common.Pair;
import impl.tgraphdb.GraphSpaceID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

public class RocksEngine implements KVEngine {

    static {
        RocksDB.loadLibrary();
    }

    private static final Log log = LogFactory.getLog(RocksEngine.class);

    private final GraphSpaceID graph;

    private final String dataPath;

    private RocksDB db = null;

    private final Options opt;


    public RocksEngine(GraphSpaceID graph, String dataPath, boolean readonly) {
        Preconditions.checkNotNull(graph);
        Preconditions.checkNotNull(dataPath);
        if (!Files.exists(Paths.get(dataPath))) {
            if (readonly) {
                log.error(String.format("Path %s not exist.", dataPath));
                System.exit(-1);
            } else {
                try {
                    Files.createDirectories(Paths.get(dataPath));
                } catch (IOException e) {
                    e.printStackTrace();
                    log.error("Create data path failed.");
                    System.exit(-1);
                }
            }
        }
        if (!Files.isDirectory(Paths.get(dataPath))) {
            log.error("Data path is not a directory");
            System.exit(-1);
        }
        this.graph = graph;
        this.dataPath = dataPath;
        this.opt = RocksEngineConfig.InitRocksdbOptions();
        if (opt == null) {
            log.error("Init Rocksdb Options Failed.");
            System.exit(-1);
        }
        if (readonly) {
            try {
                db = RocksDB.openReadOnly(opt, dataPath);
            } catch (RocksDBException e) {
                e.printStackTrace();
                log.error(String.format("open %s for read failed.", graph.getGraphName()));
                System.exit(-1);
            }
        } else {
            try {
                db = RocksDB.open(opt, dataPath);
            } catch (RocksDBException e) {
                e.printStackTrace();
                log.error(String.format("open %s failed.", graph.getGraphName()));
                System.exit(-1);
            }
        }
        Preconditions.checkNotNull(db);
        log.info(String.format("Construct RocksEngine succeed, belongs to graph %s.", graph.getGraphName()));
    }

    @Override
    public void stop() {
        if (db != null) {
            db.cancelAllBackgroundWork(true);
            db.close();
        }
        if (opt != null) {
            opt.close();
        }
        log.info(String.format("Stop RocksEngine succeed, belongs to graph %s.", graph.getGraphName()));
    }

    @Override
    public String getRoot() {
        return dataPath;
    }

    @Override
    public WriteBatch startBatchWrite() {
        return new RocksWriteBatch();
    }

    @Override
    public boolean commitBatchWrite(WriteBatch batch, boolean disableWAL, boolean sync, boolean wait) {
        try (final WriteOptions writeOpt = new WriteOptions()) {
            writeOpt.setDisableWAL(disableWAL);
            writeOpt.setSync(sync);
            writeOpt.setNoSlowdown(!wait);
            try {
                RocksWriteBatch wb = (RocksWriteBatch) batch;
                db.write(writeOpt, wb.getWb());
            } catch (RocksDBException e) {
                e.printStackTrace();
                log.error("Commit WriteBatch Failed.");
                return false;
            }
        }
        return true;
    }

    @Override
    public Object getSnapshot() {
        return db.getSnapshot();
    }

    @Override
    public void releaseSnapshot(Object snapshot) {
        db.releaseSnapshot((Snapshot) snapshot);
    }

    @Override
    public byte[] get(byte[] key, Object snapshot) {
        Snapshot snap = (Snapshot) snapshot;
        try (ReadOptions readOptions = new ReadOptions()) {
            if (snap != null) {
                readOptions.setSnapshot(snap);
            }
            return db.get(readOptions, key);
        } catch (RocksDBException e) {
            e.printStackTrace();
            log.error(String.format("Get key %s failed.", Arrays.toString(key)));
            System.exit(-1);
        }
        return null;
    }

    @Override
    public List<byte[]> multiGet(List<byte[]> keys) {
        try (ReadOptions readOptions = new ReadOptions()) {
            return db.multiGetAsList(readOptions, keys);
        } catch (RocksDBException e) {
            e.printStackTrace();
            log.error("MultiGet failed.");
        }
        return null;
    }

    @Override
    public byte[] getForPrev(byte[] key, Object snapshot) {
        Snapshot snap = (Snapshot) snapshot;
        try (ReadOptions readOptions = new ReadOptions()) {
            if (snap != null) {
                readOptions.setSnapshot(snap);
            }
            try (var iter = db.newIterator(readOptions)) {
                if (iter.isValid()) {
                    iter.seekForPrev(key);
                    if (iter.isValid()) {
                        return iter.value();
                    }
                }
            }
        }
        return null;
    }

    @Override
    public List<byte[]> multiGetForPrev(List<byte[]> keys) {
        try (var iter = db.newIterator()) {
            ArrayList<byte[]> values = new ArrayList<>();
            for (var key : keys) {
                iter.seekForPrev(key);
                values.add(iter.isValid() ? iter.value() : null);
            }
            return values;
        }
    }

    @Override
    public KVIterator range(byte[] start, byte[] end) {
        var iter = db.newIterator();
        if (iter != null) {
            iter.seek(start);
            return new RocksRangeIterator(start, end, iter);
        }
        return null;
    }

    @Override
    public KVIterator prefix(byte[] prefix, Object snapshot) {
        try (ReadOptions readOptions = new ReadOptions()) {
            if (snapshot != null) {
                readOptions.setSnapshot((Snapshot) snapshot);
            }
            //readOptions.setPrefixSameAsStart(true);
            var iter = db.newIterator(readOptions);
            if (iter != null) {
                iter.seek(prefix);
            }
            return new RocksPrefixIterator(prefix, iter);
        }
    }

    @Override
    public KVIterator rangeWithPrefix(byte[] start, byte[] prefix) {
        try (ReadOptions readOptions = new ReadOptions()) {
            // we should guarantee the order.
            readOptions.setTotalOrderSeek(true);
            var iter = db.newIterator(readOptions);
            if (iter != null) {
                iter.seek(start);
            }
            return new RocksPrefixIterator(prefix, iter);
        }
    }

    @Override
    public boolean put(byte[] key, byte[] value) {
        try (WriteOptions writeOptions = new WriteOptions()) {
            writeOptions.setDisableWAL(RocksEngineConfig.rocksdb_disable_wal);
            try {
                db.put(writeOptions, key, value);
                return true;
            } catch (RocksDBException e) {
                e.printStackTrace();
                log.error(String.format("Put key %s failed.", Arrays.toString(key)));
            }
        }
        return false;
    }

    @Override
    public boolean multiPut(List<Pair<byte[], byte[]>> keyValues) {
        try (org.rocksdb.WriteBatch wb = new org.rocksdb.WriteBatch(RocksEngineConfig.rocksdb_batch_size)) {
            for (var pr : keyValues) {
                try {
                    wb.put(pr.first(), pr.second());
                } catch (RocksDBException e) {
                    e.printStackTrace();
                    log.error(String.format("MultiPut failed in position key %s.", Arrays.toString(pr.first())));
                    return false;
                }
            }
            try (WriteOptions writeOptions = new WriteOptions()) {
                writeOptions.setDisableWAL(RocksEngineConfig.rocksdb_disable_wal);
                try {
                    db.write(writeOptions, wb);
                    return true;
                } catch (RocksDBException e) {
                    e.printStackTrace();
                    log.error("MultiPut failed.");
                }
            }
        }
        return false;
    }

    @Override
    public boolean remove(byte[] key) {
        try (WriteOptions writeOptions = new WriteOptions()) {
            writeOptions.setDisableWAL(RocksEngineConfig.rocksdb_disable_wal);
            try {
                db.delete(writeOptions, key);
                return true;
            } catch (RocksDBException e) {
                e.printStackTrace();
                log.error(String.format("Delete key %s failed.", Arrays.toString(key)));
            }
        }
        return false;
    }

    @Override
    public boolean multiRemove(List<byte[]> keys) {
        try (org.rocksdb.WriteBatch wb = new org.rocksdb.WriteBatch(RocksEngineConfig.rocksdb_batch_size)) {
            for (var key : keys) {
                try {
                    wb.delete(key);
                } catch (RocksDBException e) {
                    e.printStackTrace();
                    log.error(String.format("MultiRemove failed in position key %s.", Arrays.toString(key)));
                    return false;
                }
            }
            try (WriteOptions writeOptions = new WriteOptions()) {
                writeOptions.setDisableWAL(RocksEngineConfig.rocksdb_disable_wal);
                try {
                    db.write(writeOptions, wb);
                    return true;
                } catch (RocksDBException e) {
                    e.printStackTrace();
                    log.error("MultiRemove failed.");
                }
            }
        }
        return false;
    }

    @Override
    public boolean removeRange(byte[] start, byte[] end) {
        try (WriteOptions writeOptions = new WriteOptions()) {
            writeOptions.setDisableWAL(RocksEngineConfig.rocksdb_disable_wal);
            try {
                db.deleteRange(writeOptions, start, end);
                return true;
            } catch (RocksDBException e) {
                e.printStackTrace();
                log.error(String.format("Delete range [%s, %s) failed.", Arrays.toString(start), Arrays.toString(end)));
            }
        }
        return false;
    }

    @Override
    public boolean flush() {
        try (FlushOptions flushOptions = new FlushOptions()) {
            db.flush(flushOptions);
            return true;
        } catch (RocksDBException e) {
            e.printStackTrace();
            log.error("flush failed.");
        }
        return false;
    }
}
