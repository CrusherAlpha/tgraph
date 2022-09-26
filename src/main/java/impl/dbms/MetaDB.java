package impl.dbms;

import common.Codec;
import common.Pair;
import impl.tgraphdb.GraphSpaceID;
import kvstore.KVEngine;
import kvstore.RocksEngine;
import kvstore.StoreOptions;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class MetaDB {
    private final KVEngine db;

    public MetaDB(String databaseDirectory) {
        GraphSpaceID metaGraph = new GraphSpaceID(0, "meta-db");
        StoreOptions opt = StoreOptions.of(metaGraph, databaseDirectory);
        this.db = new RocksEngine(opt);
    }

    boolean put(String databaseName, Long graphID) {
        return db.put(databaseName.getBytes(Charset.defaultCharset()), Codec.encodeValue(graphID));
    }

    String get(String databaseName) {
        var ret = db.get(databaseName.getBytes(Charset.defaultCharset()), null);
        return ret == null ? null : new String(ret);
    }

    boolean delete(String databaseName) {
        return db.remove(databaseName.getBytes(Charset.defaultCharset()));
    }

    List<Pair<String, Long>> scan() {
        var all = db.scan();
        List<Pair<String, Long>> ret = new ArrayList<>();
        for (var pr : all) {
            ret.add(Pair.of(new String(pr.first()), (Long) Codec.decodeValue(pr.second())));
        }
        return ret;
    }

}
