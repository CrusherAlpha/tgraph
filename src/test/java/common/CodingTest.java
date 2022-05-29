package common;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static common.Coding.encode;
import static common.Coding.decode;

public class CodingTest {
    @Test
    void testPrimitive() {
        // boolean
        assertEquals(true, decode(encode(true)));
        assertEquals(false, decode(encode(false)));
        // byte
        byte b = 'a';
        assertEquals(b, decode(encode(b)));
        // short
        short s = 10;
        assertEquals(s, decode(encode(s)));
        // int
        int i = 12;
        assertEquals(i, decode(encode(i)));
        // long
        long l = 16;
        assertEquals(l, decode(encode(l)));
        // float
        float f = 1.0f;
        assertEquals(f, decode(encode(f)));
        // double
        double d = 1.2;
        assertEquals(d, decode(encode(d)));
        // char
        char c = 'c';
        assertEquals(c, decode(encode(c)));
    }
    @Test
    void testPrimitiveArray() {
        // boolean
        boolean[] b = {true, false, true, false};
        assertArrayEquals(b, (boolean[]) decode(encode(b)));
        // byte
        byte[] bytes = {'a', 'b', 'c'};
        assertArrayEquals(bytes, (byte[]) decode(encode(bytes)));
        // short
        short[] shorts = {1, 2, 3};
        assertArrayEquals(shorts, (short[]) decode(encode(shorts)));
        // int
        int[] ints = {1, 2, 3};
        assertArrayEquals(ints, (int[]) decode(encode(ints)));
        // long
        long[] longs = {1, 2, 3};
        assertArrayEquals(longs, (long[]) decode(encode(longs)));
        // float
        float[] floats = {1.0f, 2.0f, 3.0f};
        assertArrayEquals(floats, (float[]) decode(encode(floats)));
        // double
        double[] doubles = {1.0, 2.0, 3.0};
        assertArrayEquals(doubles, (double[]) decode(encode(doubles)));
        // char
        char[] chars = {'a', 'b', 'c'};
        assertArrayEquals(chars, (char[]) decode(encode(chars)));
    }
    @Test
    void testString() {
        String s = "crusher";
        assertEquals(s, decode(encode(s)));
        String[] str = {"louie", "crusher", "alpha"};
        assertArrayEquals(str, (String[]) decode(encode(str)));
    }
    // TODO(crusher): test more.
}
