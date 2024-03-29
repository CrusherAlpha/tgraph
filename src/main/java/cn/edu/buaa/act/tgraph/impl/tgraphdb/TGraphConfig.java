package cn.edu.buaa.act.tgraph.impl.tgraphdb;

// Config TGraph runtime config.
public class TGraphConfig {
    // time unit: seconds
    public static int DEADLOCK_DETECT_INTERVAL = 20;

    public static int BACKGROUND_THREAD_POOL_THREAD_NUMBER = 6;

    public static int MAX_CONCURRENT_TRANSACTION_NUMS = 800;

    // commit log
    public static String COMMIT_LOG_NODE_LABEL = "COMMIT_FLAG";
    public static String COMMIT_LOG_TXN_IDENTIFIER = "TXN_ID";

    // purge
    public static int PURGE_BATCH_SIZE = 100_000;
    // time unit: seconds
    public static int PURGE_INTERVAL = 5;


    // temporal property
    public static String TEMPORAL_PROPERTY_PREFIX = "t_";
    public static byte TEMPORAL_PROPERTY_VALUE_PLACEHOLDER = (byte) 0xff;
}
