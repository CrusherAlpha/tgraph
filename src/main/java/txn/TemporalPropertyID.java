package txn;

import com.google.common.base.Preconditions;

import java.util.Objects;

// unify vertex and edge by introducing an additional variable.
public class TemporalPropertyID {
    private final long startNodeId;
    private final long endNodeId;
    private final String propertyName;

    private TemporalPropertyID(long startNodeId, long endNodeId, String propertyName) {
        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
        this.propertyName = propertyName;
    }

    public long getStartNodeId() {
        return startNodeId;
    }

    public long getEndNodeId() {
        Preconditions.checkState(endNodeId != -1, "vertex should not have end node id.");
        return endNodeId;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public static TemporalPropertyID vertex(long vertexId, String propertyName) {
        return new TemporalPropertyID(vertexId, -1, propertyName);
    }

    public static TemporalPropertyID edge(long startNodeId, long endNodeId, String propertyName) {
        return new TemporalPropertyID(startNodeId, endNodeId, propertyName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TemporalPropertyID that = (TemporalPropertyID) o;
        return startNodeId == that.startNodeId && endNodeId == that.endNodeId && Objects.equals(propertyName, that.propertyName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startNodeId, endNodeId, propertyName);
    }

    @Override
    public String toString() {
        return "TemporalPropertyID{" +
                "startNodeId=" + startNodeId +
                ", endNodeId=" + endNodeId +
                ", propertyName='" + propertyName + '\'' +
                '}';
    }
}
