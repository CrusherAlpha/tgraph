package api.tgraphdb;

/**
 * A node in the graph with properties and relationships to other entities.
 * Along with relationships, nodes are the core building
 * blocks of the Neo4j data representation model. Nodes are created by invoking
 * the Transaction#createNode method.
 * <p>
 * Node has three major groups of operations: operations that deal with
 * relationships, operations that deal with properties (see Entity) and
 * operations that deal with labels.
 * <p>
 * The relationship operations provide a number of overloaded accessors (such as
 * <code>getRelationships(...)</code> with "filters" for type, direction, etc),
 * as well as the factory method createRelationshipTo(...) that connects two
 * nodes with a relationship. It also includes the convenience method
 * getSingleRelationship(...) for accessing the commonly occurring
 * one-to-zero-or-one association.
 * <p>
 * The property operations give access to the key-value property pairs. Property
 * keys are always strings. Valid property value types are all the Java
 * primitives (<code>int</code>, <code>byte</code>, <code>float</code>, etc),
 * <code>java.lang.String</code>s and arrays of primitives and Strings.
 * <p>
 * <b>Please note</b> that TGraph does NOT accept arbitrary objects as property
 * values.setProperty() takes a <code>java.lang.Object</code> only to avoid
 * an explosion of overloaded * <code>setProperty()</code> methods.
 * For further documentation see Entity.
 * <p>
 * A node's id is unique, but note the following: Neo4j reuses its internal ids
 * when nodes and relationships are deleted, which means it's bad practice to
 * refer to them this way. Instead, use application generated ids.
 */
public interface Node extends Entity {
    /**
     * Deletes this node if it has no relationships attached to it. If
     * <code>delete()</code> is invoked on a node with relationships, an
     * unchecked exception will be raised when the transaction is committing.
     * Invoking any methods on this node after <code>delete()</code> has
     * returned is invalid and will lead to NotFoundException being thrown.
     */
    void delete();

    /**
     * Returns all the relationships attached to this node. If no relationships
     * are attached to this node, an empty iterable will be returned.
     *
     * @return all relationships attached to this node
     */
    Iterable<Relationship> getRelationships();

    /**
     * Returns <code>true</code> if there are any relationships attached to this
     * node, <code>false</code> otherwise.
     *
     * @return <code>true</code> if there are any relationships attached to this
     * node, <code>false</code> otherwise
     */
    boolean hasRelationship();

    /**
     * Returns all the relationships of any of the types in <code>types</code>
     * that are attached to this node, regardless of direction. If no
     * relationships of the given types are attached to this node, an empty
     * iterable will be returned.
     *
     * @param types the given relationship type(s)
     * @return all relationships of the given type(s) that are attached to this
     * node
     */
    Iterable<Relationship> getRelationships(RelationshipType... types);

    /**
     * Returns all the relationships of any of the types in <code>types</code>
     * that are attached to this node and have the given <code>direction</code>.
     * If no relationships of the given types are attached to this node, an empty
     * iterable will be returned.
     *
     * @param types     the given relationship type(s)
     * @param direction the direction of the relationships to return.
     * @return all relationships of the given type(s) that are attached to this
     * node
     */
    Iterable<Relationship> getRelationships(Direction direction, RelationshipType... types);

    /**
     * Returns <code>true</code> if there are any relationships of any of the
     * types in <code>types</code> attached to this node (regardless of
     * direction), <code>false</code> otherwise.
     *
     * @param types the given relationship type(s)
     * @return <code>true</code> if there are any relationships of any of the
     * types in <code>types</code> attached to this node,
     * <code>false</code> otherwise
     */
    boolean hasRelationship(RelationshipType... types);

    /**
     * Returns <code>true</code> if there are any relationships of any of the
     * types in <code>types</code> attached to this node (for the given
     * <code>direction</code>), <code>false</code> otherwise.
     *
     * @param types     the given relationship type(s)
     * @param direction the direction to check relationships for
     * @return <code>true</code> if there are any relationships of any of the
     * types in <code>types</code> attached to this node,
     * <code>false</code> otherwise
     */
    boolean hasRelationship(Direction direction, RelationshipType... types);

    /**
     * Returns all Direction#OUTGOING or INCOMING relationships from this node.
     * If there are no relationships with the given direction attached to this
     * node, an empty iterable will be returned. If Direction#BOTH
     * is passed in as a direction, relationships of both directions are
     * returned (effectively turning this into <code>getRelationships()</code>).
     *
     * @param dir the given direction, where <code>Direction.OUTGOING</code>
     *            means all relationships that have this node as start node and
     *            <code> Direction.INCOMING</code>
     *            means all relationships that have this node as end node
     * @return all relationships with the given direction that are attached to this node
     */
    Iterable<Relationship> getRelationships(Direction dir);

    /**
     * Returns <code>true</code> if there are any relationships in the given
     * direction attached to this node, <code>false</code> otherwise. If
     * Direction#BOTH is passed in as a direction, relationships of
     * both directions are matched (effectively turning this into
     * <code>hasRelationships()</code>).
     *
     * @param dir the given direction, where <code>Direction.OUTGOING</code>
     *            means all relationships that have this node as
     *            start node and <code>Direction.INCOMING</code>
     *            means all relationships that have this node as end node
     * @return <code>true</code> if there are any relationships in the given
     * direction attached to this node, <code>false</code> otherwise
     */
    boolean hasRelationship(Direction dir);

    /**
     * Returns the only relationship of a given type and direction that is
     * attached to this node, or <code>null</code>. This is a convenience method
     * that is used in the commonly occurring situation where a node has exactly
     * zero or one relationship of a given type and direction to another node.
     * Typically, this invariant is maintained by the rest of the code: if at any
     * time more than one such relationships exist, it is a fatal error that
     * should generate an unchecked exception. This method reflects that
     * semantics and returns either:
     * <ol>
     * <li><code>null</code> if there are zero relationships of the given type
     * and direction,</li>
     * <li>the relationship if there's exactly one, or
     * <li>throws an unchecked exception in all other cases.</li>
     * </ol>
     * <p>
     * This method should be used only in situations with an invariant as
     * described above. In those situations, a "state-checking" method (e.g.
     * <code>hasSingleRelationship(...)</code>) is not required, because this
     * method behaves correctly "out of the box."
     *
     * @param type the type of the wanted relationship
     * @param dir  the direction of the wanted relationship (where
     *             <code>Direction.OUTGOING</code> means a relationship that has
     *             this node as start node and <code>Direction.INCOMING</code>
     *             means a relationship that has this node as end node or
     *             Direction#BOTH if direction is irrelevant
     * @return the single relationship matching the given type and direction if
     * exactly one such relationship exists, or <code>null</code> if
     * exactly zero such relationships exists
     * @throws RuntimeException if more than one relationship matches the given
     *                          type and direction
     */
    Relationship getSingleRelationship(RelationshipType type, Direction dir);

    /**
     * Creates a relationship between this node and another node. The
     * relationship is of type <code>type</code>. It starts at this node and
     * ends at <code>otherNode</code>.
     * <p>
     * A relationship is equally well traversed in both directions so there's no
     * need to create another relationship in the opposite direction (in regards
     * to traversal or performance).
     *
     * @param otherNode the end node of the new relationship
     * @param type      the type of the new relationship
     * @return the newly created relationship
     */
    Relationship createRelationshipTo(Node otherNode, RelationshipType type);

    /**
     * Returns relationship types which this node has one more relationships
     * for. If this node doesn't have any relationships an empty {@link Iterable}
     * will be returned.
     *
     * @return relationship types which this node has one more relationships for.
     */
    Iterable<RelationshipType> getRelationshipTypes();

    /**
     * Returns the number of relationships connected to this node regardless of
     * direction or type. This operation is always O(1).
     *
     * @return the number of relationships connected to this node.
     */
    int getDegree();

    /**
     * Returns the number of relationships of a given {@code type} connected to this node.
     *
     * @param type the type of relationships to get the degree for
     * @return the number of relationships of a given {@code type} connected to this node.
     */
    int getDegree(RelationshipType type);

    /**
     * Returns the number of relationships of a given {@code direction} connected to this node.
     *
     * @param direction the direction of the relationships
     * @return the number of relationships of a given {@code direction} for this node.
     */
    int getDegree(Direction direction);

    /**
     * Returns the number of relationships of a given {@code type} and {@code direction}
     * connected to this node.
     *
     * @param type      the type of relationships to get the degree for
     * @param direction the direction of the relationships
     * @return the number of relationships of a given {@code type} and {@code direction}
     * for this node.
     */
    int getDegree(RelationshipType type, Direction direction);

    /**
     * Adds a Label to this node. If this node doesn't already have
     * this label it will be added. If it already has the label, nothing will happen.
     *
     * @param label the label to add to this node.
     */
    void addLabel(Label label);

    /**
     * Removes a Label from this node. If this node doesn't have this label,
     * nothing will happen.
     *
     * @param label the label to remove from this node.
     */
    void removeLabel(Label label);

    /**
     * Checks whether this node has the given label.
     *
     * @param label the label to check for.
     * @return {@code true} if this node has the given label, otherwise {@code false}.
     */
    boolean hasLabel(Label label);

    /**
     * Lists all labels attached to this node. If this node has no
     * labels an empty Iterable will be returned.
     *
     * @return all labels attached to this node.
     */
    Iterable<Label> getLabels();
}