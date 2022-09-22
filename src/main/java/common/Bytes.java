package common;

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
}
