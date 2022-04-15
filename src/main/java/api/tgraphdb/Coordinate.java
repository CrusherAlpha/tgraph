package api.tgraphdb;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


import static java.util.Arrays.stream;

/**
 * A coordinate is used to describe a position in space.
 * <p>
 * A coordinate is described by at least two numbers and must adhere to the following ordering
 * <ul>
 * <li>x, y, z ordering in a cartesian reference system</li>
 * <li>east, north, altitude in a projected coordinate reference system</li>
 * <li>longitude, latitude, altitude in a geographic reference system</li>
 * </ul>
 * <p>
 * Additional numbers are allowed and the meaning of these additional numbers depends on the coordinate reference
 * system
 * (see CRS)
 */
public final class Coordinate {
    private final double[] coordinate;

    public Coordinate(double... coordinate) {
        if (coordinate.length < 2) {
            throw new IllegalArgumentException("A coordinate must have at least two elements");
        }
        this.coordinate = coordinate;
    }

    /**
     * Returns the current coordinate.
     *
     * @return A list of numbers describing the coordinate.
     */
    public List<Double> getCoordinate() {
        return stream(coordinate).boxed().collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Coordinate that = (Coordinate) o;

        return Arrays.equals(coordinate, that.coordinate);

    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(coordinate);
    }
}
