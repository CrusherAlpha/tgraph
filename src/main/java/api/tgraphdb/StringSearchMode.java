package api.tgraphdb;

/**
 * The string search mode is used together with a value template to find nodes of interest.
 * The search mode can be one of:
 * <ul>
 *   <li>EXACT: The value has to match the template exactly.</li>
 *   <li>PREFIX: The value must have a prefix matching the template.</li>
 *   <li>SUFFIX: The value must have a suffix matching the template.</li>
 *   <li>CONTAINS: The value must contain the template. Only exact matches are supported.</li>
 * </ul>
 */
public enum StringSearchMode {
    /**
     * The value has to match the template exactly.
     */
    EXACT,
    /**
     * The value must have a prefix matching the template.
     */
    PREFIX,
    /**
     * The value must have a suffix matching the template.
     */
    SUFFIX,
    /**
     * The value must contain the template exactly. Regular expressions are not supported.
     */
    CONTAINS
}
