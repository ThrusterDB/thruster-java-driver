package org.trueno.driver.lib.core.data_structures;

public enum Algorithm {
    DEPENDENCIES("Dependencies"),
    PAGE_RANK("Page Rank"),
    WORD_COUNT("Word Count"),
    TRIANGLE_COUNTING("Triangle Counting"),
    CONNECTED_COMPONENTS("Connected Components"),
    STRONGLY_CONNECTED_COMPONENTS("Strongly Connected Components"),
    SHORTEST_PATHS("Shortest Paths"),
    NONE("None");

    private final String text;

    /**
     * Sets the status of the operation.
     *
     * @param text Status string representation.
     */
    Algorithm(final String text) {
        this.text = text;
    }

    /* (non-Javadoc)
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString() {
        return text;
    }
}
