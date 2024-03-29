package cn.edu.buaa.act.tgraph.impl.dbms;

import cn.edu.buaa.act.tgraph.common.Codec;
import cn.edu.buaa.act.tgraph.common.Pair;
import cn.edu.buaa.act.tgraph.impl.tgraphdb.GraphSpaceID;
import cn.edu.buaa.act.tgraph.kvstore.KVEngine;
import cn.edu.buaa.act.tgraph.kvstore.RocksEngine;
import cn.edu.buaa.act.tgraph.kvstore.StoreOptions;

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

    Long get(String key) {
        var ret = db.get(Codec.encodeString(key), null);
        return ret == null ? null : Codec.bytesToLong(ret);
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
