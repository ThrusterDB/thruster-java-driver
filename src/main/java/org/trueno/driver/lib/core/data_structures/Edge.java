package org.trueno.driver.lib.core.data_structures;

import org.jdeferred.Promise;
import org.json.JSONException;
import org.json.JSONObject;
import org.trueno.driver.lib.core.communication.Message;

/**
 * <b>Edge Class</b>
 * <p>TruenoDB Edge primitive data structure class.</p>
 *
 * @author Victor Santos
 * @author Miguel Rivera
 * @version 0.1.0
 */
public class Edge extends Component {

    /**
     * Initializes a new Edge
     */
    public Edge() {
        try {
            this.put("source", "");
            this.put("target", "");
            this.put("partition", "");

            this.setType("e");

        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while instantiating a new Edge.", ex);
        }
    }

    /**
     * Returns the source of this Edge
     *
     * @return Edge source
     */
    public String getSource() {
        try {
            return this.get("source").toString();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while getting the source of an edge.", ex);
        }
    }

    /**
     * Sets the source of this Edge
     *
     * @param source
     *         new Edge source
     */
    public void setSource(String source) {
        try {
            this.put("source", source);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the source of an edge.", ex);
        }
    }

    /**
     * Returns whether the source of this Edge is set
     *
     * @return true if source is set, false otherwise.
     */
    boolean hasSource() {
        try {
            return this.has("source") && !this.get("source").toString().isEmpty();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while checking the source of an edge.", ex);
        }
    }

    /**
     * Returns the target of this Edge
     *
     * @return Edge target
     */
    public String getTarget() {
        try {
            return this.get("target").toString();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while getting the target of an edge.", ex);
        }
    }

    /**
     * Sets the target of this Edge
     *
     * @param target
     *         new Edge target
     */
    public void setTarget(String target) {
        try {
            this.put("source", target);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the target of an edge.", ex);
        }
    }

    /**
     * Returns whether the target of this Edge is set
     *
     * @return true if targe is set, false otherwise.
     */
    boolean hasTarget() {
        try {
            return this.has("target") && !this.get("target").toString().isEmpty();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while checking the target of an edge.", ex);
        }
    }

    /**
     * Returns the partition of this Edge
     *
     * @return Edge partition
     */
    public String getPartition() {
        try {
            return this.get("partition").toString();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while getting the partition of an edge.", ex);
        }
    }

    /**
     * Sets the partition of this Edge
     *
     * @param partition
     *         new Edge partition
     */
    public void setPartition(String partition) {
        try {
            this.put("partition", partition);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the partition of an edge.", ex);
        }
    }

    /**
     * Returns a new filter to be applied on this Edge
     *
     * @return new Edge filter
     */
    public Filter filter() {
        return new Filter();
    }

    /**
     * Returns the vertices connected to this Edge
     *
     * @return Promise with the vertices search result.
     */
    public Promise<JSONObject, JSONObject, Integer> vertices() {
        final String apiFun = "ex_vertices";

        if (!this.hasId()) {
            throw new Error("Edge id is required, set this edge instance id or load edge.");
        }

        this.validateGraphLabel();

        Message msg = new Message();
        JSONObject payload = new JSONObject();

        try {
            payload.put("graph", this.getParentGraph().getLabel());
            payload.put("id", this.getId());

            msg.setPayload(payload);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while constructing JSON Object - vertices.", ex);
        }

        if (this.getDebug()) {
            printDebug("vertices", apiFun, payload.toString());
        }

        return this.getParentGraph().getConn().call(apiFun, msg);
    }
}
