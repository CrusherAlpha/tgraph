package impl.dbms;

import common.Codec;
import common.Pair;
import impl.tgraphdb.GraphSpaceID;
import kvstore.KVEngine;
import kvstore.RocksEngine;
import kvstore.StoreOptions;

import java.util.ArrayList;
import java.util.List;

// <String, long>: <databaseName, graphID>
public class MetaDB {
    private final KVEngine db;

    public MetaDB(String databaseDirectory) {
        GraphSpaceID metaGraph = new GraphSpaceID(0, "meta-db", databaseDirectory);
        StoreOptions opt = StoreOptions.of(metaGraph, databaseDirectory);
        this.db = new RocksEngine(opt);
    }

    void put(String key, long value) {
        db.put(Codec.encodeString(key), Codec.longToBytes(value));
    }

    String get(String key) {
        var ret = db.get(Codec.encodeString(key), null);
        return ret == null ? null : Codec.decodeString(ret);
    }

    void delete(String key) {
        db.remove(Codec.encodeString(key));
    }

    List<Pair<String, Long>> scan() {
        var all = db.scan();
        List<Pair<String, Long>> ret = new ArrayList<>();
        for (var pr : all) {
            String key = Codec.decodeString(pr.first());
            long val = Codec.bytesToLong(pr.second());
            ret.add(Pair.of(key, val));
        }
        return ret;
    }

}
