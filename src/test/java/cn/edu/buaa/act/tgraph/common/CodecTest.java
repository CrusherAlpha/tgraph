package cn.edu.buaa.act.tgraph.common;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CodecTest {
    @Test
    void testPrimitive() {
        // boolean
        Assertions.assertEquals(true, Codec.decodeValue(Codec.encodeValue(true)));
        Assertions.assertEquals(false, Codec.decodeValue(Codec.encodeValue(false)));
        // byte
        byte b = 'a';
        Assertions.assertEquals(b, Codec.decodeValue(Codec.encodeValue(b)));
        // short
        short s = 10;
        Assertions.assertEquals(s, Codec.decodeValue(Codec.encodeValue(s)));
        // int
        int i = 12;
        Assertions.assertEquals(i, Codec.decodeValue(Codec.encodeValue(i)));
        // long
        long l = 16;
        Assertions.assertEquals(l, Codec.decodeValue(Codec.encodeValue(l)));
        // float
        float f = 1.0f;
        Assertions.assertEquals(f, Codec.decodeValue(Codec.encodeValue(f)));
        // double
        double d = 1.2;
        Assertions.assertEquals(d, Codec.decodeValue(Codec.encodeValue(d)));
        // char
        char c = 'c';
        Assertions.assertEquals(c, Codec.decodeValue(Codec.encodeValue(c)));
    }
    @Test
    void testPrimitiveArray() {
        // boolean
        boolean[] b = {true, false, true, false};
        assertArrayEquals(b, (boolean[]) Codec.decodeValue(Codec.encodeValue(b)));
        // byte
        byte[] bytes = {'a', 'b', 'c'};
        assertArrayEquals(bytes, (byte[]) Codec.decodeValue(Codec.encodeValue(bytes)));
        // short
        short[] shorts = {1, 2, 3};
        assertArrayEquals(shorts, (short[]) Codec.decodeValue(Codec.encodeValue(shorts)));
        // int
        int[] ints = {1, 2, 3};
        assertArrayEquals(ints, (int[]) Codec.decodeValue(Codec.encodeValue(ints)));
        // long
        long[] longs = {1, 2, 3};
        assertArrayEquals(longs, (long[]) Codec.decodeValue(Codec.encodeValue(longs)));
        // float
        float[] floats = {1.0f, 2.0f, 3.0f};
        assertArrayEquals(floats, (float[]) Codec.decodeValue(Codec.encodeValue(floats)));
        // double
        double[] doubles = {1.0, 2.0, 3.0};
        assertArrayEquals(doubles, (double[]) Codec.decodeValue(Codec.encodeValue(doubles)));
        // char
        char[] chars = {'a', 'b', 'c'};
        assertArrayEquals(chars, (char[]) Codec.decodeValue(Codec.encodeValue(chars)));
    }
    @Test
    void testString() {
        String s = "crusher";
        Assertions.assertEquals(s, Codec.decodeValue(Codec.encodeValue(s)));
        String[] str = {"louie", "crusher", "alpha"};
        assertArrayEquals(str, (String[]) Codec.decodeValue(Codec.encodeValue(str)));
    }

    @Test
    void testPrefixNotConsistencyCodec() {
        String prefix = "crusher-";
        String k0 = prefix + "k0";
        String k1 = prefix + "k1";
        assertFalse(Bytes.startsWith(Codec.encodeValue(k0), Codec.encodeValue(prefix)));
        assertFalse(Bytes.startsWith(Codec.encodeValue(k1), Codec.encodeValue(prefix)));
    }
}
