package common;

public class Bytes {
    public static boolean startWith(byte[] src, byte[] patten) {
        if (patten.length > src.length) {
            return false;
        }
        for (int i = 0; i < patten.length; ++i) {
            if (patten[i] != src[i]) {
                return false;
            }
        }
        return true;
    }
}
