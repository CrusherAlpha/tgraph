package api.dbms;

public interface Configuration {
    /**
     * Retrieve the value of a configuration Setting.
     *
     * @param setting The configuration property
     * @param <T>     The type of the configuration property
     * @return The value of the configuration property if the property is found, otherwise, return the default value
     * of the given property.
     */
    <T> T get(Setting<T> setting);

    /**
     * Empty configuration without any settings.
     */
    Configuration EMPTY = new Configuration() {
        @Override
        public <T> T get(Setting<T> setting) {
            return null;
        }
    };
}
