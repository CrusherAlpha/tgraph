package kvstore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.*;
import org.rocksdb.util.BytewiseComparator;

import java.util.HashMap;

// we can also config rocksdb through configuration file.
// TODO(crusher): maybe it is not the best practice, improve it.
public class RocksEngineConfig {
    // Whether to disable the WAL in rocksdb
    public static boolean rocksdb_disable_wal = false;

    // Whether WAL writes are synchronized to disk or not
    public static boolean rocksdb_wal_sync = false;

    // Default reserved bytes for one batch operation
    public static int rocksdb_batch_size = 4 * 1024;

    // Default writer buffer size for rocksdb memtable. The unit is KB
    public static int write_buffer_size = 4 * 1024;

    // Default writer buffer number
    public static int write_buffer_num = 3;

    //NOTE!: we only use BlockBasedTable format in rocksdb.

    // The default block cache size used in BlockBasedTable. The unit is MB
    public static long rocksdb_block_cache = 1024;

    // Disable page cache to better control memory used by rocksdb
    public static boolean disable_page_cache = false;

    // Total keys inside the cache
    public static int rocksdb_row_cache_num = 16 * 1000 * 1000;

    // Compression algorithm used by rocksdb
    // options: no, snappy, lz4, lz4hc, zstd, zlib, bzip2, xpress
    public static String rocksdb_compression = "zstd";

    // Whether to enable rocksdb's statistics
    public static boolean enable_rocksdb_statistics = false;

    // Number of total compaction threads. 0 means unlimited.
    public static int num_compaction_threads = 0;

    // Write limit in bytes per sec. The unit is MB. 0 means unlimited
    public static long rocksdb_rate_limit = 0;

    // Whether to enable BlocDB(rocksdb key-value separation support)
    public static boolean rocksdb_enable_kv_separation = false;

    // Rocksdb key value separation threshold in bytes.
    public static int rocksdb_kv_separation_threshold = 100;

    static final HashMap<String, CompressionType> compressionTypeHashMap = new HashMap<String, CompressionType>() {
        {
            put("no", CompressionType.NO_COMPRESSION);
            put("snappy", CompressionType.SNAPPY_COMPRESSION);
            put("lz4", CompressionType.LZ4_COMPRESSION);
            put("lz4hc", CompressionType.LZ4HC_COMPRESSION);
            put("zstd", CompressionType.ZSTD_COMPRESSION);
            put("zlib", CompressionType.ZLIB_COMPRESSION);
            put("bzip2", CompressionType.BZLIB2_COMPRESSION);
            put("xpress", CompressionType.XPRESS_COMPRESSION);
            put("disable", CompressionType.DISABLE_COMPRESSION_OPTION);
        }
    };

    private static final Log log = LogFactory.getLog(RocksEngineConfig.class);

    // NOTE!: caller should guarantee to close it.
    // We don't set custom comparator here, leave it to upper layer.
    static Options InitRocksdbOptions() {

        // set the general compression algorithm
        var compress = compressionTypeHashMap.get(rocksdb_compression);
        if (compress == null) {
            log.error("Unsupported compression type: " + rocksdb_compression);
            return null;
        }
        final Options opt = new Options();
        opt.setCreateIfMissing(true);

        opt.setCompressionType(compress);

        // set the kv separation config
        if (rocksdb_enable_kv_separation) {
            opt.setEnableBlobFiles(true);
            opt.setMinBlobSize(rocksdb_kv_separation_threshold);
        }

        if (disable_page_cache) {
            opt.setUseDirectReads(true);
        }

        if (num_compaction_threads > 0) {
            var tasks = new ConcurrentTaskLimiterImpl("COMPACTION_THREAD", num_compaction_threads);
            opt.setCompactionThreadLimiter(tasks);
        }

        if (rocksdb_rate_limit > 0) {
            opt.setRateLimiter(new RateLimiter(rocksdb_rate_limit * 1024 * 1024));
        }

        BlockBasedTableConfig block_opt = new BlockBasedTableConfig();


        if (rocksdb_block_cache <= 0) {
            block_opt.setNoBlockCache(true);
        } else {
            var cache = new LRUCache(rocksdb_block_cache * 1024 * 1024);
            block_opt.setBlockCache(cache);
        }

        block_opt.setFilterPolicy(new BloomFilter(10, false));

        opt.setTableFormatConfig(block_opt);

        if (rocksdb_row_cache_num > 0) {
            opt.setRowCache(new LRUCache(rocksdb_row_cache_num));
        }

        opt.setCompactionStyle(CompactionStyle.UNIVERSAL);

        opt.setWriteBufferSize(write_buffer_size);

        opt.setMaxWriteBufferNumber(write_buffer_num);

        if (enable_rocksdb_statistics) {
            opt.setStatistics(new Statistics());
        } else {
            opt.setStatsDumpPeriodSec(0);
        }

        return opt;
    }



}
