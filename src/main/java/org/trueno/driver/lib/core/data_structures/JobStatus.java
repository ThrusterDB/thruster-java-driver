package org.trueno.driver.lib.core.data_structures;

public enum JobStatus {
    STARTED("STARTED"),
    FINISHED("FINISHED"),
    RUNNING("RUNNING"),
    ERROR("ERROR");


    private final String text;

    /**
     * Sets the status of the operation.
     *
     * @param text Status string representation.
     */
    JobStatus(final String text) {
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
