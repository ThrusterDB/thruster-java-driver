package org.trueno.driver.lib.core.communication;

import org.json.JSONException;
import org.json.JSONObject;

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

        try {
            /* Init meta map */
            this.put("_meta", new JSONObject());
            /* Init meta map */
            this.put("_payload", new JSONObject());
            /* Setting property fields */
            this.put("_type", "");
            /* Setting property fields */
            this.put("_status", "");
        } catch (JSONException e) {
            throw new RuntimeException("An error occurred while instantiating a new Message.", e);
        }

    }

    /**
     * Returns the meta attribute on the Message
     * @return meta attribute value
     */
    public JSONObject getMeta() {
        try {
            return (JSONObject) this.get("_meta");
        } catch (JSONException e) {
            throw new RuntimeException("An error occurred while manipulating JSON value.", e);
        }
    }

    /**
     * Returns the message payload
     * @return the message payload value
     */
    public JSONObject getPayload() {
        try {
            return (JSONObject) this.get("_payload");
        } catch (JSONException e) {
            throw new RuntimeException("An error occurred while manipulating JSON value.", e);
        }
    }

    /**
     * Returns the message type
     * @return the message type value
     */
    public String getType() {
        try {
            return (String) this.get("_type");
        } catch (JSONException e) {
            throw new RuntimeException("An error occurred while manipulating JSON value.", e);
        }
    }

    /**
     * Returns the message status
     * @return the message status value
     */
    public String getStatus() {
        try {
            return (String) this.get("_status");
        } catch (JSONException e) {
            throw new RuntimeException("An error occurred while manipulating JSON value.", e);
        }
    }

    /**
     * Sets the message metadata
     * @param meta the new value of the Message metadata
     */
    public void setMeta(JSONObject meta) {
        try {
            this.put("_meta", meta);
        } catch (JSONException e) {
            throw new RuntimeException("An error occurred while manipulating JSON value.", e);
        }
    }

    /**
     * Sets the message payload
     * @param payload the new value of the Message payload
     */
    public void setPayload(JSONObject payload) {
        try {
            this.put("_payload", payload);
        } catch (JSONException e) {
            throw new RuntimeException("An error occurred while manipulating JSON value.", e);
        }
    }

    /**
     * Sets the message type
     * @param type the new value of the Message type
     */
    public void getType(String type) {
        try {
            this.put("_type", type);
        } catch (JSONException e) {
            throw new RuntimeException("An error occurred while manipulating JSON value.", e);
        }
    }

    /**
     * Sets the message status
     * @param status the new value of the Message status
     */
    public void setStatus(String status) {
        try {
            this.put("_status", status);
        } catch (JSONException e) {
            throw new RuntimeException("An error occurred while manipulating JSON value.", e);
        }
    }

}
