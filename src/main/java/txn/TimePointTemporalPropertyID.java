package txn;

import java.util.Objects;

public class TimePointTemporalPropertyID {
    private final TemporalPropertyID tp;
    private final long timestamp;

    private TimePointTemporalPropertyID(TemporalPropertyID tp, long timestamp) {
        this.tp = tp;
        this.timestamp = timestamp;
    }

    public TemporalPropertyID getTp() {
        return tp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public static TimePointTemporalPropertyID fromTemporalPropertyID(TemporalPropertyID tp) {
        return new TimePointTemporalPropertyID(tp, -1);
    }

    public static TimePointTemporalPropertyID of(TemporalPropertyID tp, long timestamp) {
        return new TimePointTemporalPropertyID(tp, timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimePointTemporalPropertyID that = (TimePointTemporalPropertyID) o;
        return timestamp == that.timestamp && Objects.equals(tp, that.tp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tp, timestamp);
    }
}
