package cn.edu.buaa.act.tgraph.api.tgraphdb;

import cn.edu.buaa.act.tgraph.common.Pair;

import java.util.List;
import java.util.Map;
import java.sql.Timestamp;

import org.neo4j.graphdb.NotFoundException;
import cn.edu.buaa.act.tgraph.txn.TransactionAbortException;

/**
 * An Entity is persisted in the database, and identified by an id.
 * Nodes and Relationships are Entities. Entities are attached to transaction in which they were accessed.
 * Outside of transaction its possible only to access entity id. All other methods should be called only in the scope
 * of the owning transaction. Defines a common API for handling properties on both nodes and relationships.
 * <p>
 * <p>
 * Properties are key-value pairs. The keys are always strings.
 * Properties are temporal and static.
 * <p>
 * The complete list of currently supported property types is:
 * boolean
 * byte
 * short
 * int
 * long
 * float
 * double
 * char
 * java.lang.String
 * java.time.LocalDate
 * java.time.OffsetTime
 * java.time.LocalTime
 * java.time.ZonedDateTime
 * It is also possible to use java.time.OffsetDateTime and it will be converted to a ZonedDateTime internally.
 * java.time.LocalDateTime
 * java.time.temporal.TemporalAmount
 * There are two concrete implementations of this interface, java.time.Duration and java.time.Period which
 * will be converted to a single Neo4j Duration type. This means loss of type information, so properties of this type,
 * when read back using getProperty will be only of type java.time.temporal.TemporalAmount.
 * Arrays of the above types, for example int[], String[] or LocalTime[]
 * Please note that TGraph does NOT accept arbitrary objects as property values. setProperty() takes a java.lang.
 * Object only to avoid an explosion of overloaded setProperty() methods.
 */

public interface Entity {
    /**
     * Returns the unique id of this entity. Ids are reused over time, so they are only guaranteed to be unique
     * during a specific transaction: if the entity is deleted, it is
     * likely that some new entity will reuse this id at some point.
     * The id is not unique between Nodes and Relationships, because they are stored in fixed size and in different files.
     *
     * @return The id of this Entity.
     */
    long getId();

    // static properties interface.
    /**
     * Returns <code>true</code> if this property container has a property
     * accessible through the given key, <code>false</code> otherwise. If key is
     * <code>null</code>, this method returns <code>false</code>.
     *
     * @param key the property key
     * @return <code>true</code> if this property container has a property
     * accessible through the given key, <code>false</code> otherwise
     */
    boolean hasProperty(String key);

    /**
     * Returns the property value associated with the given key. The value is of
     * one of the valid property types.
     * <p>
     * If there's no property associated with <code>key</code> an unchecked
     * exception is raised. The idiomatic way to avoid an exception for an
     * unknown key and instead get <code>null</code> back is to use a default
     * value: Object valueOrNull = nodeOrRel.getProperty(key, null)
     *
     * @param key the property key
     * @return the property value associated with the given key
     * @throws NotFoundException if there's no property associated with
     *                           <code>key</code>
     */
    Object getProperty(String key);

    /**
     * Returns the property value associated with the given key, or a default
     * value.
     *
     * @param key          the property key
     * @param defaultValue the default value that will be returned if no
     *                     property value was associated with the given key
     * @return the property value associated with the given key
     */
    Object getProperty(String key, Object defaultValue);

    /**
     * Sets the property value for the given key to <code>value</code>. The
     * property value must be one of the valid property types.
     * <p>
     * This means that <code>null</code> is not an accepted property value.
     *
     * @param key   the key with which the new property value will be associated
     * @param value the new property value, of one of the valid property types
     * @throws IllegalArgumentException if <code>value</code> is of an
     *                                  unsupported type (including <code>null</code>)
     */
    void setProperty(String key, Object value);

    /**
     * Removes the property associated with the given key and returns the old
     * value. If there's no property associated with the key, <code>null</code>
     * will be returned.
     *
     * @param key the property key
     * @return the property value that used to be associated with the given key
     */
    Object removeProperty(String key);

    /**
     * Returns all existing property keys, or an empty iterable if this property
     * container has no properties.
     *
     * @return all property keys on this property container
     */
    // TODO: figure out concurrency semantics
    Iterable<String> getPropertyKeys();

    /**
     * Returns specified existing properties. The collection is mutable,
     * but changing it has no impact on the graph as the data is detached.
     *
     * @param keys the property keys to return
     * @return specified properties on this property container
     * @throws NullPointerException if the array of keys or any key is null
     */
    Map<String, Object> getProperties(String... keys);

    /**
     * Returns all existing static properties.
     *
     * @return all static properties on this property container
     */
    Map<String, Object> getAllProperties();

    // temporal property extension.
    // temporal property encoding:
    // key: t_{temporal property key} value: 0xff
    // user only need to pass {temporal property key}.
    // we store {key, value} pair in Neo4j as a placeholder to build the connection
    // between topology storage and temporal property storage.
    // NOTE!: timestamp precision is millisecond.

    /**
     * Create a temporal property.
     *
     * @param key the property key
     * @throws TemporalPropertyExistsException if property already exists
     */
    void createTemporalProperty(String key);

    /**
     * Returns whether a temporal property exists.
     *
     * @param key the property key
     * @return true if exists or false if not exists
     */
    boolean hasTemporalProperty(String key);

    /**
     * Returns specified existing temporal property value in timestamp.
     *
     * @param key       the property key
     * @param timestamp the timestamp
     * @return the property value associated with timestamp
     * @throws TemporalPropertyNotExistsException if property not exist
     * @throws TransactionAbortException if internal errors(deadlock etc.) occur
     */
    Object getTemporalPropertyValue(String key, Timestamp timestamp) throws TransactionAbortException;

    /**
     * Returns specified existing temporal property values in time range [start, end).
     *
     * @param key   the property key
     * @param start the start timestamp
     * @param end   the end timestamp
     * @return a list of property values associated with timestamp
     * @throws IllegalArgumentException           if <code>start</code> is bigger than or equal to <code>end</code>
     * @throws TemporalPropertyNotExistsException if property not exists
     * @throws TransactionAbortException if internal errors(deadlock etc.) occur
     */
    List<Pair<Timestamp, Object>> getTemporalPropertyValue(String key, Timestamp start, Timestamp end) throws TransactionAbortException;

    /**
     * Sets the property value for the given key associated with timestamp
     * to <code>value</code>. The property value must be one of the valid
     * property types.
     * <p>
     * This means that <code>null</code> is not an accepted property value.
     *
     * @param key       the key with which the new property value will be associated
     * @param timestamp the timestamp of the temporal property
     * @param value     the property value, of one of the valid property types
     * @throws IllegalArgumentException           if <code>value</code> is of an
     *                                            unsupported type (including <code>null</code>)
     * @throws TemporalPropertyNotExistsException if property not exists
     * @throws TransactionAbortException if internal errors(deadlock etc.) occur
     */
    void setTemporalPropertyValue(String key, Timestamp timestamp, Object value) throws TransactionAbortException;

    /**
     * Sets the property value for the given key between [start, end)
     * to <code>value</code>. The property value must be one of the valid
     * property types.
     * <p>
     * This means that <code>null</code> is not an accepted property value.
     *
     * @param key   the key with which the new property value will be associated
     * @param start the start timestamp of the temporal property
     * @param end   the end timestamp of the temporal property
     * @param value the property value, of one of the valid property types
     * @throws IllegalArgumentException           if <code>start</code> is bigger than or equal to <code>end</code>
     * @throws TemporalPropertyNotExistsException if property not exists
     * @throws TransactionAbortException if internal errors(deadlock etc.) occur
     */
    void setTemporalPropertyValue(String key, Timestamp start, Timestamp end, Object value) throws TransactionAbortException;

    /**
     * Removes all the property values and this temporal property.
     *
     * @param key the property key
     * @throws TemporalPropertyNotExistsException if property not exists
     * @throws TransactionAbortException if internal errors(deadlock etc.) occur
     */
    void removeTemporalProperty(String key) throws TransactionAbortException;

    /**
     * Removes the property value associated with timestamp.
     *
     * @param key       the property key
     * @param timestamp the timestamp of this temporal property
     * @throws TemporalPropertyNotExistsException if property not exists.
     * @throws TransactionAbortException if internal errors(deadlock etc.) occur
     */
    void removeTemporalPropertyValue(String key, Timestamp timestamp) throws TransactionAbortException;

    /**
     * Removes the property value between [start, end).
     *
     * @param key   the property key
     * @param start the start timestamp of this temporal property
     * @param end   the end timestamp of this temporal property
     * @throws TemporalPropertyNotExistsException if property not exists.
     * @throws IllegalArgumentException           if <code>start</code> is bigger than or equal to <code>end</code>
     * @throws TransactionAbortException if internal errors(deadlock etc.) occur
     */
    void removeTemporalPropertyValue(String key, Timestamp start, Timestamp end) throws TransactionAbortException;

    /**
     * Removes all the property value but not delete the property.
     *
     * @param key the property key
     * @throws TemporalPropertyNotExistsException if property not exists.
     * @throws TransactionAbortException if internal errors(deadlock etc.) occur
     */
    void removeTemporalPropertyValue(String key) throws TransactionAbortException;

    /**
     * Returns all existing temporal properties.
     *
     * @return all temporal properties on this property container
     */
    Iterable<String> getTemporalPropertyKeys();
}