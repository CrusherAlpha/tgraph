package api.tgraphdb;

/**
 * A point is a geometry described by a single coordinate in space.
 * <p>
 * A call to getCoordinates() must return a single element list.
 */
public interface Point extends Geometry {
    /**
     * Returns the single coordinate in space defining this point.
     *
     * @return The coordinate of this point.
     */
    default Coordinate getCoordinate() {
        return getCoordinates().get(0);
    }

    @Override
    default String getGeometryType() {
        return "Point";
    }
}
