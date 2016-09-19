package org.trueno.driver.lib.core.communication;

enum Status {
    SUCCESS("success"),
    ERROR("error");

    private final String text;

    /**
     * @param text Status string representation
     */
    Status(final String text) {
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