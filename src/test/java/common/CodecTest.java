package common;


import org.junit.jupiter.api.Test;

import static common.Codec.*;
import static org.junit.jupiter.api.Assertions.*;

public class CodecTest {
    @Test
    void testPrimitive() {
        // boolean
        assertEquals(true, decodeValue(encodeValue(true)));
        assertEquals(false, decodeValue(encodeValue(false)));
        // byte
        byte b = 'a';
        assertEquals(b, decodeValue(encodeValue(b)));
        // short
        short s = 10;
        assertEquals(s, decodeValue(encodeValue(s)));
        // int
        int i = 12;
        assertEquals(i, decodeValue(encodeValue(i)));
        // long
        long l = 16;
        assertEquals(l, decodeValue(encodeValue(l)));
        // float
        float f = 1.0f;
        assertEquals(f, decodeValue(encodeValue(f)));
        // double
        double d = 1.2;
        assertEquals(d, decodeValue(encodeValue(d)));
        // char
        char c = 'c';
        assertEquals(c, decodeValue(encodeValue(c)));
    }
    @Test
    void testPrimitiveArray() {
        // boolean
        boolean[] b = {true, false, true, false};
        assertArrayEquals(b, (boolean[]) decodeValue(encodeValue(b)));
        // byte
        byte[] bytes = {'a', 'b', 'c'};
        assertArrayEquals(bytes, (byte[]) decodeValue(encodeValue(bytes)));
        // short
        short[] shorts = {1, 2, 3};
        assertArrayEquals(shorts, (short[]) decodeValue(encodeValue(shorts)));
        // int
        int[] ints = {1, 2, 3};
        assertArrayEquals(ints, (int[]) decodeValue(encodeValue(ints)));
        // long
        long[] longs = {1, 2, 3};
        assertArrayEquals(longs, (long[]) decodeValue(encodeValue(longs)));
        // float
        float[] floats = {1.0f, 2.0f, 3.0f};
        assertArrayEquals(floats, (float[]) decodeValue(encodeValue(floats)));
        // double
        double[] doubles = {1.0, 2.0, 3.0};
        assertArrayEquals(doubles, (double[]) decodeValue(encodeValue(doubles)));
        // char
        char[] chars = {'a', 'b', 'c'};
        assertArrayEquals(chars, (char[]) decodeValue(encodeValue(chars)));
    }
    @Test
    void testString() {
        String s = "crusher";
        assertEquals(s, decodeValue(encodeValue(s)));
        String[] str = {"louie", "crusher", "alpha"};
        assertArrayEquals(str, (String[]) decodeValue(encodeValue(str)));
    }

    @Test
    void testPrefixNotConsistencyCodec() {
        String prefix = "crusher-";
        String k0 = prefix + "k0";
        String k1 = prefix + "k1";
        assertFalse(Bytes.startsWith(encodeValue(k0), encodeValue(prefix)));
        assertFalse(Bytes.startsWith(encodeValue(k1), encodeValue(prefix)));
    }
}
