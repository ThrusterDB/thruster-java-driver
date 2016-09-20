package org.trueno.driver.lib.core.data_structures;

import org.json.JSONException;
import org.json.JSONObject;
import org.trueno.driver.lib.core.communication.Message;

import java.util.concurrent.CompletableFuture;

/**
 * Created by: victor, miguel
 * Date: 7/19/16
 * Purpose:
 */

class Edge extends Component {

    Edge() {
        try {
            this.put("source", "");
            this.put("target", "");
            this.put("partition",  "");

            this.setType("e");

        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while instantiating a new Edge.", ex);
        }
    }

    public String getSource() {
        try {
            return this.get("source").toString();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while getting the source of an edge.", ex);
        }
    }

    public void setSource(String source) {
        try {
            this.put("source", source);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the source of an edge.", ex);
        }
    }

    boolean hasSource() {
        try {
            return this.has("source") && !this.get("source").toString().isEmpty();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while checking the source of an edge.", ex);
        }
    }

    public String getTarget() {
        try {
            return this.get("target").toString();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while getting the target of an edge.", ex);
        }
    }

    public void setTarget(String target) {
        try {
            this.put("source", target);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the target of an edge.", ex);
        }
    }

    boolean hasTarget() {
        try {
            return this.has("target") && !this.get("target").toString().isEmpty();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while checking the target of an edge.", ex);
        }
    }

    public String getPartition() {
        try {
            return this.get("partition").toString();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while getting the partition of an edge.", ex);
        }
    }

    public void setPartition(String partition) {
        try {
            this.put("partition", partition);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the partition of an edge.", ex);
        }
    }

    Filter filter() {
        return new Filter();
    }

    CompletableFuture<JSONObject> vertices() {
        final String apiFun = "ex_vertices";

        if(!this.hasId()) {
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

        return this.getParentGraph().getConn().call(apiFun, msg).handleAsync((ret, err) -> {
            if (ret != null)
                return ret;
            else
                throw new RuntimeException("Error occurred while fulfilling destroy promise", err);
        });
    }
}
