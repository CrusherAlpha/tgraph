package api.tgraphdb;

/**
 * A label is a grouping facility for Node where all nodes having a label
 * are part of the same group. Labels on nodes are optional and any node can
 * have an arbitrary number of labels attached to it.
 * <p>
 * Objects of classes implementing this interface can be used as label
 * representations in your code.
 * <p>
 * It's very important to note that a label is uniquely identified
 * by its name, not by any particular instance that implements this interface.
 * This means that the proper way to check if two labels are equal
 * is by invoking <code>equals()</code> on their name() names, NOT by
 * using Java's identity operator (<code>==</code>) or <code>equals()</code> on
 * the Label instances. A consequence of this is that you can NOT
 * use Label instances in hashed collections such as
 * java.util.HashMap and java.util.HashSet.
 * <p>
 * However, you usually want to check whether a specific node
 * <i>instance</i> has a certain label. That is best achieved with the
 * Node#hasLabel(Label) method.
 * <p>
 * For labels that your application know up front you should specify using an enum,
 * and since the name is accessed using the name() method it fits nicely.
 * <code>
 * public enum MyLabels implements Label
 * {
 * PERSON,
 * RESTAURANT;
 * }
 * </code>
 * <p>
 * For labels that your application don't know up front you can make use of
 * label(String), or your own implementation of this interface,
 * as it's just the name that matters.
 */
public interface Label {
    /**
     * Returns the name of the label. The name uniquely identifies a
     * label, i.e. two different Label instances with different object identifiers
     * (and possibly even different classes) are semantically equivalent if they
     * have String#equals(Object) equal names.
     *
     * @return the name of the label
     */
    String name();

    /**
     * Instantiates a new Label with the given name.
     *
     * @param name the name of the label
     * @return a Label instance for the given name
     * @throws IllegalArgumentException if name is {@code null}
     */
    static Label label(String name) {
        if (name == null) {
            throw new IllegalArgumentException("A label cannot have a null name");
        }
        return new Label() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public String toString() {
                return name;
            }

            @Override
            public boolean equals(Object that) {
                if (this == that) {
                    return true;
                }
                if (that == null || that.getClass() != getClass()) {
                    return false;
                }
                return name.equals(((Label) that).name());
            }

            @Override
            public int hashCode() {
                return name.hashCode();
            }
        };
    }
}
