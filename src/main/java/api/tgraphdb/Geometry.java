package api.tgraphdb;

import java.util.List;

/**
 * A geometry is defined by a list of coordinates and a coordinate reference system.
 */
public interface Geometry {
    /**
     * Get string description of most specific type of this instance
     *
     * @return The instance type implementing Geometry
     */
    String getGeometryType();

    /**
     * Get all coordinates of the geometry.
     *
     * @return The coordinates of the geometry.
     */
    List<Coordinate> getCoordinates();

    /**
     * Returns the coordinate reference system associated with the geometry
     *
     * @return A CRS associated with the geometry
     */
    CRS getCRS();
}
