package common;

public class Pair<T, U> {
    private final T first;
    private final U second;
    Pair(T first, U second) {
        this.first = first;
        this.second = second;
    }

    public static <T, U> Pair<T, U> of(T first, U second) {
        return new Pair<T, U>(first, second);
    }

    public T first() {
        return first;
    }

    public U second() {
        return second;
    }
}
