package cn.edu.buaa.act.tgraph.common;


import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PairTest {
    @Test
    void testPairBase() {
        String sf = "crusher";
        Integer is = 22;
        var pair = Pair.of(sf, is);
        System.out.println(pair);
        assertEquals(sf, pair.first());
        assertEquals(is, pair.second());
    }

    @Test
    void testPairTimestamp() {
        long datetime = System.currentTimeMillis();
        Timestamp timestamp = new Timestamp(datetime);
        String val = "crusher";
        var pair = Pair.of(timestamp, val);
        assertEquals(pair.first().getTime(), datetime);
    }

}
