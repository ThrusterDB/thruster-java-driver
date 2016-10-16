package org.trueno.driver.lib.core.communication;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <b>Message Class</b>
 * <p>Class used to format messages passed between the driver and the remote Trueno Database</p>
 *
 * @author Victor Santos
 * @author Miguel Rivera
 * @version 0.1.0
 */
public class Message extends JSONObject {
    /**
     * Initializes a Message with empty fields
     */
    public Message() {
        /* Init meta map */
        this.put("_meta", new JSONObject());
        /* Init meta map */
        this.put("_payload", new JSONObject());
        /* Setting property fields */
        this.put("_type", "");
        /* Setting property fields */
        this.put("_status", "");
    }

    /**
     * Returns the meta attribute on the Message
     *
     * @return meta attribute value
     */
    public JSONObject getMeta() {
        return (JSONObject) this.get("_meta");
    }

    /**
     * Returns the message payload
     *
     * @return the message payload value
     */
    public JSONObject getPayload() {
        return (JSONObject) this.get("_payload");
    }

    /**
     * Returns the message type
     *
     * @return the message type value
     */
    public String getType() {
        return this.get("_type").toString();
    }

    /**
     * Returns the message status
     *
     * @return the message status value
     */
    public String getStatus() {
        return this.get("_status").toString();
    }

    /**
     * Sets the message metadata
     *
     * @param meta the new value of the Message metadata
     */
    public void setMeta(JSONObject meta) {
        this.put("_meta", meta);
    }

    /**
     * Sets the message payload
     *
     * @param payload the new value of the Message payload
     */
    public void setPayload(JSONObject payload) {
        this.put("_payload", payload);
    }

    /**
     * Returns the message type
     *
     * @param type the Message type
     */
    public void getType(String type) {
        this.put("_type", type);
    }

    /**
     * Sets the message status
     *
     * @param status the new value of the Message status
     */
    public void setStatus(String status) {
        this.put("_status", status);
    }
}
