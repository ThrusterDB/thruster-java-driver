package org.trueno.driver.lib.core.communication;

/**
 * Enumerator for Socket-IO Emitter return codes.
 *
 * @author Victor Santos
 * @author Miguel Rivera
 * @version 0.1.0
 */
enum Status {
    SUCCESS("success"),
    ERROR("error");

    private final String text;

    /**
     * Sets the status of the operation.
     *
     * @param text
     *         Status string representation.
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