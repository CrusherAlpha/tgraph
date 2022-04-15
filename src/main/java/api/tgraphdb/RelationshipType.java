package api.tgraphdb;

/**
 * A relationship type is mandatory on all relationships and is used to navigate
 * the graph. RelationshipType is  used in various relationship operations on Node.
 * <p>
 * Relationship types are declared by the client and can be handled either
 * dynamically or statically in a TGraph-based application. Internally,
 * relationship types are dynamic. This means that every time a client invokes
 * node.createRelationshipTo(anotherNode, newRelType) and passes in a new
 * relationship type then the new type will be transparently created. So
 * instantiating a RelationshipType instance will not create it in the
 * underlying storage, it is persisted only when the first relationship of that
 * type is created.
 * <p>
 * However, in case the application does not need to dynamically create
 * relationship types (most don't), then it's nice to have the compile-time
 * benefits of a static set of relationship types. Fortunately, RelationshipType
 * is designed to work well with Java 5 enums. This means that it's very easy to
 * define a set of valid relationship types by declaring an enum that implements
 * RelationshipType and then reuse that across the application. For example,
 * here's how you would define an enum to hold all your relationship types:
 *
 * <pre>
 * <code>
 * enum MyRelationshipTypes implements RelationshipType
 * {
 *     CONTAINED_IN, KNOWS
 * }
 * </code>
 * </pre>
 * <p>
 * Then later, it's as easy to use as:
 *
 * <pre>
 * <code>
 * node.createRelationshipTo(anotherNode, MyRelationshipTypes.KNOWS);
 * for (Relationship rel : node.getRelationships(MyRelationshipTypes.KNOWS))
 * {
 *     // ...
 * }
 * </code>
 * </pre>
 *
 * <p>
 * It's very important to note that a relationship type is uniquely identified
 * by its name, not by any particular instance that implements this interface.
 * This means that the proper way to check if two relationship types are equal
 * is by invoking <code>equals()</code> on their name, NOT by
 * using Java's identity operator (<code>==</code>) or <code>equals()</code> on
 * the relationship type instances. A consequence of this is that you can NOT
 * use relationship types in hashed collections such as
 * java.util.HashMap and java.util.HashSet.
 * <p>
 * However, you usually want to check whether a specific relationship
 * <i>instance</i> is of a certain type. That is best achieved with the
 * Relationship.isType method, such as:
 * <pre>
 * <code>
 * if (rel.isType(MyRelationshipTypes.CONTAINED_IN))
 * {
 *     ...
 * }
 * </code>
 * </pre>
 */
public interface RelationshipType {
    /**
     * Returns the name of the relationship type. The name uniquely identifies a
     * relationship type, i.e. two different RelationshipType instances with
     * different object identifiers (and possibly even different classes) are
     * semantically equivalent if they have String#equals(Object) names.
     *
     * @return the name of the relationship type
     */
    String name();

    /**
     * Instantiates a new RelationshipType with the given name.
     *
     * @param name the name of the dynamic relationship type
     * @return a relationshiptype with the given name
     * @throws IllegalArgumentException if name is {@code null}
     */
    static RelationshipType withName(String name) {
        if (name == null) {
            throw new IllegalArgumentException("A relationship type cannot have a null name");
        }
        return new RelationshipType() {
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
                return name.equals(((RelationshipType) that).name());
            }

            @Override
            public int hashCode() {
                return name.hashCode();
            }
        };
    }
}
