package cn.edu.buaa.act.tgraph.common;

import com.google.common.base.Predicate;
import cn.edu.buaa.act.tgraph.impl.tgraphdb.TGraphConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EntityUtil {

    private static boolean temporal(String key) {
        return key.startsWith(TGraphConfig.TEMPORAL_PROPERTY_PREFIX);
    }

    public static Iterable<String> staticPropertyKeysFilter(Iterable<String> keys) {
        List<String> ret = new ArrayList<>();
        keys.forEach(ret::add);
        return ret.stream().filter((Predicate<String>) s -> !temporal(s)).collect(Collectors.toList());
    }

    public static Iterable<String> temporalPropertyKesFilter(Iterable<String> keys) {
        List<String> ret = new ArrayList<>();
        keys.forEach(ret::add);
        return ret.stream().filter((Predicate<String>) EntityUtil::temporal).collect(Collectors.toList());
    }

    public static Map<String, Object> staticPropertiesFilter(Map<String, Object> kvs) {
        HashMap<String, Object> ret = new HashMap<>();
        for (var entry : kvs.entrySet()) {
            if (!temporal(entry.getKey())) {
                ret.put(entry.getKey(), entry.getValue());
            }
        }
        return ret;
    }

    public static String temporalPropertyWrapper(String key) {
        return TGraphConfig.TEMPORAL_PROPERTY_PREFIX + key;
    }
}
