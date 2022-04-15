package api.dbms;

/**
 * Settings that can be provided in configurations are represented by instances of this interface.
 *
 * @param <T> The type of the values associated with this setting.
 */
public interface Setting<T> {
    /**
     * The full (unique) name, identifying a specific setting.
     *
     * @return the name.
     */
    String name();

    /**
     * The default value of this setting
     *
     * @return the typed default value.
     */
    T defaultValue();

    /**
     * A dynamic setting have its value changed in a config at any time
     *
     * @return true if the setting is dynamic, false otherwise
     */
    boolean dynamic();

    /**
     * An internal setting should not be accessed nor altered by any user
     * Internal settings may be changed or removed between versions without notice
     *
     * @return true if the setting is internal, false otherwise
     */
    boolean internal();

    /**
     * A textual representation describing the usage if this setting
     *
     * @return the description of this setting
     */
    String description();
}
