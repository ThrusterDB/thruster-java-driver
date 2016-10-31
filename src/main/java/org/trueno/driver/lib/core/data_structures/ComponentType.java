package org.trueno.driver.lib.core.data_structures;

public enum ComponentType {
    GRAPH("g"),
    VERTEX("v"),
    EDGE("e"),
    COMPUTE("c"),
    UNDEFINED("");

    private final String text;

    /**
     * Sets the status of the operation.
     *
     * @param text Status string representation.
     */
    ComponentType(final String text) {
        this.text = text;
    }

    /* (non-Javadoc)
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString() {
        return text;
    }

    public boolean invalid() { return this.equals(UNDEFINED); }
}
