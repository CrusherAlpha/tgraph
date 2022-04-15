package api.tgraphdb;

/**
 * A coordinate reference system (CRS) determines how a Coordinate should be interpreted
 * <p>
 * The CRS is defined by three properties a code, a type, and a link to CRS parameters on the web.
 * Example:
 * <code>
 * {
 * code: 4326,
 * type: "WGS-84",
 * href: "http://spatialreference.org/ref/epsg/4326/"
 * }
 * </code>
 */
public interface CRS {

    /**
     * The numerical code associated with the CRS
     *
     * @return a numerical code associated with the CRS
     */
    int getCode();

    /**
     * The type of the CRS is a descriptive name, indicating which CRS is used
     *
     * @return the type of the CRS
     */
    String getType();

    /**
     * A link uniquely identifying the CRS.
     *
     * @return A link to where the CRS is described.
     */
    String getHref();
}
