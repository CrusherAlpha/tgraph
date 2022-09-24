package common;

import java.nio.ByteBuffer;

public class Bytes {
    public static boolean startsWith(byte[] key, byte[] prefix) {
        if (key == prefix) {
            return true;
        }
        if (key == null || prefix == null) {
            return false;
        }
        if (prefix.length > key.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; ++i) {
            if (prefix[i] != key[i]) {
                return false;
            }
        }
        return true;
    }

    public static int memcmp(final ByteBuffer x, final ByteBuffer y, final int start, final int count) {
        for (int idx = start; idx < start + count; ++idx) {
            final int aa = x.get(idx) & 0xff;
            final int bb = y.get(idx) & 0xff;
            if (aa != bb) {
                return aa - bb;
            }
        }
        return 0;
    }
}
