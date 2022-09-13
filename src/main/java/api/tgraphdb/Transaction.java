package api.tgraphdb;

import org.neo4j.graphdb.*;

import java.util.Map;

/**
 * A programmatically handled transaction.
 * All database operations that access the temporal graph,  must be performed in a transaction.
 * If you attempt to access the graph outside a transaction, those operations will throw NotInTransactionException.
 * Here's the idiomatic use of programmatic transactions in Neo4j:
 * <p>
 * try (Transaction txn = tgraphDb.beginTx())
 * {
 * // operations on the graph
 * // ...
 * <p>
 * txn.commit();
 * }
 * <p>
 * Let's walk through this example line by line. First we retrieve a Transaction
 * object by invoking the TGraphDatabaseService#beginTx() factory method.
 * This creates a new transaction which has internal state to keep
 * track of whether the current transaction is successful. Then we wrap all
 * operations that modify the graph in a try-finally block with the transaction
 * as resource. At the end of the block, we invoke the commit() tx.commit()
 * method to commit that the transaction.
 * <p>
 * If an exception is raised in the try-block, commit() will never be
 * invoked and the transaction will be rolled backed. This is very important:
 * unless commit() is invoked, the transaction will fail upon
 * close(). A transaction can be explicitly rolled back by
 * invoking the rollback() method.
 * <p>
 * Read operations inside a transaction will also read uncommitted data from
 * the same transaction.
 * <p>
 * All ResourceIterables that were returned from operations executed inside a transaction
 * will be automatically closed when the transaction is committed or rolled back.
 * Note however, that the ResourceIterator should be closed as soon as
 * possible if you don't intend to exhaust the iterator.
 * <p>
 * Note that transactions should be used by a single thread only.
 * It is generally not safe to use a transaction from multiple threads.
 * Doing so will lead to undefined behavior.
 */
public interface Transaction extends AutoCloseable {

    /**
     * Creates a new node.
     *
     * @return the created node.
     */
    Node createNode();

    /**
     * Creates a new node and adds the provided labels to it.
     *
     * @param labels labels to add to the created node.
     * @return the created node.
     */
    Node createNode(Label... labels);

    /**
     * Looks up a node by id. Please note: Neo4j reuses its internal ids when
     * nodes and relationships are deleted, which means it's bad practice
     * referring to them this way. Instead, use application generated ids.
     *
     * @param id the id of the node
     * @return the node with id <code>id</code> if found
     * @throws NotFoundException if not found
     */
    Node getNodeById(long id);

    /**
     * Looks up a relationship by id. Please note: Neo4j reuses its internal ids
     * when nodes and relationships are deleted, which means it's bad practice
     * referring to them this way. Instead, use application generated ids.
     *
     * @param id the id of the relationship
     * @return the relationship with id <code>id</code> if found
     * @throws NotFoundException if not found
     */
    Relationship getRelationshipById(long id);


    /**
     * Returns all labels currently in the underlying store. Labels are added to the store the first time
     * they are used. This method guarantees that it will return all labels currently in use.
     * <p>
     * Please take care that the returned ResourceIterable is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return all labels in the underlying store.
     */
    Iterable<Label> getAllLabelsInUse();

    /**
     * Returns all relationship types currently in the underlying store.
     * Relationship types are added to the underlying store the first time they
     * are used in a successfully committed node.createRelationshipTo(...).
     * This method guarantees that it will return all relationship types currently in use.
     *
     * @return all relationship types in the underlying store
     */
    Iterable<RelationshipType> getAllRelationshipTypesInUse();

    /**
     * Returns all labels currently in the underlying store. Labels are added to the store the first time
     * they are used. This method guarantees that it will return all labels currently in use. However,
     * it may also return <i>more</i> than that (e.g. it can return "historic" labels that are no longer used).
     * <p>
     * Please take care that the returned ResourceIterable is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return all labels in the underlying store.
     */
    Iterable<Label> getAllLabels();

    /**
     * Returns all relationship types currently in the underlying store.
     * Relationship types are added to the underlying store the first time they
     * are used in a successfully committed node.createRelationshipTo(...).
     * Note that this method is guaranteed to
     * return all known relationship types, but it does not guarantee that it
     * won't return <i>more</i> than that (e.g. it can return "historic"
     * relationship types that no longer have any relationships in the node
     * space).
     *
     * @return all relationship types in the underlying store
     */
    Iterable<RelationshipType> getAllRelationshipTypes();

    /**
     * Returns all property keys currently in the underlying store. This method guarantees that it will return all
     * property keys currently in use. However, it may also return <i>more</i> than that (e.g. it can return "historic"
     * labels that are no longer used).
     * <p>
     * Please take care that the returned ResourceIterable is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return all property keys in the underlying store.
     */
    Iterable<String> getAllPropertyKeys();

    /**
     * Returns all nodes having a given label, and a property value of type String or Character matching the
     * given value template and search mode.
     * <p>
     * If an online index is found, it will be used to look up the requested nodes.
     * If no indexes exist for the label/property combination, the database will
     * scan all labeled nodes looking for matching property values.
     * <p>
     * The search mode and value template are used to select nodes of interest. The search mode can
     * be one of
     * <ul>
     *   <li>EXACT: The value has to match the template exactly. This is the same behavior as {@link Transaction#findNode(Label, String, Object)}.</li>
     *   <li>PREFIX: The value must have a prefix matching the template.</li>
     *   <li>SUFFIX: The value must have a suffix matching the template.</li>
     *   <li>CONTAINS: The value must contain the template. Only exact matches are supported.</li>
     * </ul>
     * Note that in Neo4j the Character 'A' will be treated the same way as the String 'A'.
     * <p>
     * Please ensure that the returned ResourceIterator is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param label      consider nodes with this label
     * @param key        required property key
     * @param template   required property value template
     * @param searchMode search mode to use for finding matches
     * @return an iterator containing all matching nodes. See ResourceIterator for responsibilities.
     */
    ResourceIterator<Node> findNodes(Label label, String key, String template, StringSearchMode searchMode);

    /**
     * Returns all nodes having the label, and the wanted property values.
     * If an online index is found, it will be used to look up the requested
     * nodes.
     * <p>
     * If no indexes exist for the label with all provided properties, the database will
     * scan all labeled nodes looking for matching nodes.
     * <p>
     * Note that equality for values do not follow the rules of Java. This means that the number 42 is equals to all
     * other 42 numbers, regardless of whether they are encoded as Integer, Long, Float, Short, Byte or Double.
     * <p>
     * Same rules follow Character and String - the Character 'A' is equal to the String 'A'.
     * <p>
     * Finally - arrays also follow these rules. An int[] {1,2,3} is equal to a double[] {1.0, 2.0, 3.0}
     * <p>
     * Please ensure that the returned ResourceIterator is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param label          consider nodes with this label
     * @param propertyValues required property key-value combinations
     * @return an iterator containing all matching nodes. See ResourceIterator for responsibilities.
     */
    ResourceIterator<Node> findNodes(Label label, Map<String, Object> propertyValues);

    /**
     * Returns all nodes having the label, and the wanted property values.
     * If an online index is found, it will be used to look up the requested
     * nodes.
     * <p>
     * If no indexes exist for the label with all provided properties, the database will
     * scan all labeled nodes looking for matching nodes.
     * <p>
     * Note that equality for values do not follow the rules of Java. This means that the number 42 is equals to all
     * other 42 numbers, regardless of whether they are encoded as Integer, Long, Float, Short, Byte or Double.
     * <p>
     * Same rules follow Character and String - the Character 'A' is equal to the String 'A'.
     * <p>
     * Finally - arrays also follow these rules. An int[] {1,2,3} is equal to a double[] {1.0, 2.0, 3.0}
     * <p>
     * Please ensure that the returned ResourceIterator is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param label  consider nodes with this label
     * @param key1   required property key1
     * @param value1 required property value of key1
     * @param key2   required property key2
     * @param value2 required property value of key2
     * @param key3   required property key3
     * @param value3 required property value of key3
     * @return an iterator containing all matching nodes. See ResourceIterator for responsibilities.
     */
    ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2, String key3, Object value3);

    /**
     * Returns all nodes having the label, and the wanted property values.
     * If an online index is found, it will be used to look up the requested
     * nodes.
     * <p>
     * If no indexes exist for the label with all provided properties, the database will
     * scan all labeled nodes looking for matching nodes.
     * <p>
     * Note that equality for values do not follow the rules of Java. This means that the number 42 is equals to all
     * other 42 numbers, regardless of whether they are encoded as Integer, Long, Float, Short, Byte or Double.
     * <p>
     * Same rules follow Character and String - the Character 'A' is equal to the String 'A'.
     * <p>
     * Finally - arrays also follow these rules. An int[] {1,2,3} is equal to a double[] {1.0, 2.0, 3.0}
     * <p>
     * Please ensure that the returned ResourceIterator is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param label  consider nodes with this label
     * @param key1   required property key1
     * @param value1 required property value of key1
     * @param key2   required property key2
     * @param value2 required property value of key2
     * @return an iterator containing all matching nodes. See ResourceIterator for responsibilities.
     */
    ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2);

    /**
     * Equivalent to {@link #findNodes(Label, String, Object)}, however it must find no more than one
     * {@link Node node} or it will throw an exception.
     *
     * @param label consider nodes with this label
     * @param key   required property key
     * @param value required property value
     * @return the matching node or <code>null</code> if none could be found
     * @throws MultipleFoundException if more than one matching node is found
     */
    Node findNode(Label label, String key, Object value);

    /**
     * Returns all nodes having the label, and the wanted property value.
     * If an online index is found, it will be used to look up the requested
     * nodes.
     * <p>
     * If no indexes exist for the label/property combination, the database will
     * scan all labeled nodes looking for the property value.
     * <p>
     * Note that equality for values do not follow the rules of Java. This means that the number 42 is equals to all
     * other 42 numbers, regardless of whether they are encoded as Integer, Long, Float, Short, Byte or Double.
     * <p>
     * Same rules follow Character and String - the Character 'A' is equal to the String 'A'.
     * <p>
     * Finally - arrays also follow these rules. An int[] {1,2,3} is equal to a double[] {1.0, 2.0, 3.0}
     * <p>
     * Please ensure that the returned ResourceIterator is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param label consider nodes with this label
     * @param key   required property key
     * @param value required property value
     * @return an iterator containing all matching nodes. See ResourceIterator for responsibilities.
     */
    ResourceIterator<Node> findNodes(Label label, String key, Object value);

    /**
     * Returns all nodes with a specific label.
     * <p>
     * Please take care that the returned ResourceIterator is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param label the Label to return nodes for.
     * @return an iterator containing all nodes matching the label. See ResourceIterator for responsibilities.
     */
    ResourceIterator<Node> findNodes(Label label);

    /**
     * Returns all relationships having the type, and a property value of type String or Character matching the
     * given value template and search mode.
     * <p>
     * If an online index is found, it will be used to look up the requested
     * relationships.
     * If no indexes exist for the type/property combination, the database will
     * scan all relationships of a specific type looking for matching property values.
     * <p>
     * The search mode and value template are used to select relationships of interest. The search mode can
     * be one of
     * <ul>
     *   <li>EXACT: The value has to match the template exactly. This is the same behavior
     *              as Transaction#findRelationships(RelationshipType, String, Object).</li>
     *   <li>PREFIX: The value must have a prefix matching the template.</li>
     *   <li>SUFFIX: The value must have a suffix matching the template.</li>
     *   <li>CONTAINS: The value must contain the template. Only exact matches are supported.</li>
     * </ul>
     * Note that in Neo4j the Character 'A' will be treated the same way as the String 'A'.
     * <p>
     * Please ensure that the returned ResourceIterator is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param relationshipType consider relationships with this type
     * @param key              required property key
     * @param template         required property value template
     * @param searchMode       search mode to use for finding matches
     * @return an iterator containing all matching relationships. See ResourceIterator for responsibilities.
     */
    ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, String key, String template, StringSearchMode searchMode);

    /**
     * Returns all relationships having the type, and the wanted property values.
     * If an online index is found, it will be used to look up the requested
     * relationships.
     * <p>
     * If no indexes exist for the type with all provided properties, the database will
     * scan all relationships of a specific type looking for matching values.
     * <p>
     * Note that equality for values do not follow the rules of Java. This means that the number 42 is equals to all
     * other 42 numbers, regardless of whether they are encoded as Integer, Long, Float, Short, Byte or Double.
     * <p>
     * Same rules follow Character and String - the Character 'A' is equal to the String 'A'.
     * <p>
     * Finally - arrays also follow these rules. An int[] {1,2,3} is equal to a double[] {1.0, 2.0, 3.0}
     * <p>
     * Please ensure that the returned ResourceIterator is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param relationshipType consider relationships with this type
     * @param propertyValues   required property key-value combinations
     * @return an iterator containing all matching relationships. See ResourceIterator for responsibilities.
     */
    ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, Map<String, Object> propertyValues);

    /**
     * Returns all relationships having the type, and the wanted property values.
     * If an online index is found, it will be used to look up the requested
     * relationships.
     * <p>
     * If no indexes exist for the type with all provided properties, the database will
     * scan all relationships of a specific type looking for matching values.
     * <p>
     * Note that equality for values do not follow the rules of Java. This means that the number 42 is equals to all
     * other 42 numbers, regardless of whether they are encoded as Integer, Long, Float, Short, Byte or Double.
     * <p>
     * Same rules follow Character and String - the Character 'A' is equal to the String 'A'.
     * <p>
     * Finally - arrays also follow these rules. An int[] {1,2,3} is equal to a double[] {1.0, 2.0, 3.0}
     * <p>
     * Please ensure that the returned ResourceIterator is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param relationshipType consider relationships with this type
     * @param key1             required property key1
     * @param value1           required property value of key1
     * @param key2             required property key2
     * @param value2           required property value of key2
     * @param key3             required property key3
     * @param value3           required property value of key3
     * @return an iterator containing all matching relationships. See ResourceIterator for responsibilities.
     */
    ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, String key1, Object value1,
                                                     String key2, Object value2, String key3, Object value3);

    /**
     * Returns all relationships having the type, and the wanted property values.
     * If an online index is found, it will be used to look up the requested
     * relationships.
     * <p>
     * If no indexes exist for the type with all provided properties, the database will
     * scan all relationships of a specific type looking for matching values.
     * <p>
     * Note that equality for values do not follow the rules of Java. This means that the number 42 is equals to all
     * other 42 numbers, regardless of whether they are encoded as Integer, Long, Float, Short, Byte or Double.
     * <p>
     * Same rules follow Character and String - the Character 'A' is equal to the String 'A'.
     * <p>
     * Finally - arrays also follow these rules. An int[] {1,2,3} is equal to a double[] {1.0, 2.0, 3.0}
     * <p>
     * Please ensure that the returned ResourceIterator is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param relationshipType consider relationships with this type
     * @param key1             required property key1
     * @param value1           required property value of key1
     * @param key2             required property key2
     * @param value2           required property value of key2
     * @return an iterator containing all matching relationships. See ResourceIterator for responsibilities.
     */
    ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, String key1, Object value1, String key2, Object value2);

    /**
     * Equivalent to findRelationships(RelationshipType, String, Object), however it must find no more than one relationship, or it
     * will throw an exception.
     *
     * @param relationshipType consider relationships with this type
     * @param key              required property key
     * @param value            required property value
     * @return the matching relationship or <code>null</code> if none could be found
     * @throws MultipleFoundException if more than one matching relationship is found
     */
    Relationship findRelationship(RelationshipType relationshipType, String key, Object value);

    /**
     * Returns all relationships having the type, and the wanted property value.
     * If an online index is found, it will be used to look up the requested
     * relationships.
     * <p>
     * If no indexes exist for the type/property combination, the database will
     * scan all relationships of a specific type looking for the property value.
     * <p>
     * Note that equality for values do not follow the rules of Java. This means that the number 42 is equals to all
     * other 42 numbers, regardless of whether they are encoded as Integer, Long, Float, Short, Byte or Double.
     * <p>
     * Same rules follow Character and String - the Character 'A' is equal to the String 'A'.
     * <p>
     * Finally - arrays also follow these rules. An int[] {1,2,3} is equal to a double[] {1.0, 2.0, 3.0}
     * <p>
     * Please ensure that the returned ResourceIterator is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param relationshipType consider relationships with this type
     * @param key              required property key
     * @param value            required property value
     * @return an iterator containing all matching relationships. See ResourceIterator for responsibilities.
     */
    ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, String key, Object value);

    /**
     * Returns all relationships of a specific type.
     * <p>
     * Please take care that the returned ResourceIterator is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param relationshipType the RelationshipType to return relationships for.
     * @return an iterator containing all relationships with matching type. See ResourceIterator for responsibilities.
     * @throws IllegalStateException if relationship index feature not enabled
     */
    ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType);

    /**
     * Marks this transaction as terminated, which means that it will be, much like in the case of failure,
     * unconditionally rolled back when close() is called. Once this method has been invoked, it doesn't matter
     * if commit() is invoked afterwards -- the transaction will still be rolled back.
     * <p>
     * Additionally, terminating a transaction causes all subsequent operations carried out within that
     * transaction to throw a TransactionTerminatedException.
     * <p>
     * Note that, unlike the other transaction operations, this method can be called from different thread.
     * When this method is called, it signals to terminate the transaction and returns immediately.
     * <p>
     * Calling this method on an already closed transaction has no effect.
     */
    void terminate();

    /**
     * Returns all nodes in the graph.
     *
     * @return all nodes in the graph.
     */
    ResourceIterable<Node> getAllNodes();

    /**
     * Returns all relationships in the graph.
     *
     * @return all relationships in the graph.
     */
    ResourceIterable<Relationship> getAllRelationships();

    /**
     * Commit and close current transaction.
     * <p>
     * When {@code commit()} is completed, all resources are released and no more changes are possible in this transaction.
     */
    void commit();

    /**
     * Roll back and close current transaction.
     * When {@code rollback()} is completed, all resources are released and no more changes are possible in this transaction
     */
    void rollback();

    /**
     * Close transaction. If commit() or rollback() have been called this does nothing.
     * If none of them are called, the transaction will be rolled back.
     *
     * <p>All ResourceIterables that where returned from operations executed inside this
     * transaction will be automatically closed by this method in they were not closed before.
     *
     * <p>This method comes from AutoCloseable so that a Transaction can participate
     * in try-with-resource statements.
     */
    @Override
    void close();

    // 2pc extension
    /**
     * @return true if ready to commit or false if not ready.
     *
     * If return true, transaction should guarantee to be able to commit in any circumstances
     * unless users send rollback command.
     */
    boolean prepare();
}
