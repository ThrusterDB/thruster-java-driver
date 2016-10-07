package org.trueno.driver.lib.core.data_structures;

import org.jdeferred.Deferred;
import org.jdeferred.DoneCallback;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.trueno.driver.lib.core.communication.Message;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Created by: victor, miguel
 * Date: 7/19/16
 * Purpose:
 */

public class Vertex extends Component {

    public Vertex() {

        try {
            this.put("partition", 0);
            this.setType("v");

        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while instantiating a new Vertex.", ex);
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

    Promise<JSONObject, JSONObject, Integer> in(String cmp, Filter filter) {
        return neighbors(cmp, filter, "in");
    }

    Promise<JSONObject, JSONObject, Integer> out(String cmp, Filter filter) {
        return neighbors(cmp, filter, "out");
    }

    public Promise<JSONObject, JSONObject, Integer> neighbors(String cmpType, Filter filter, String direction) {
        final String apiFun = "ex_neighbors";

        this.validateCmp(cmpType);

        if (!this.hasId()) {
            throw new Error("Vertex ID is required.");
        }

        this.validateGraphLabel();

        Message msg = new Message();
        JSONObject payload = new JSONObject();

        try {
            payload.put("graph", this.getParentGraph().getLabel());
            payload.put("id", this.getId());
            payload.put("dir", direction);
            payload.put("cmp", cmpType.toLowerCase());

            if (filter != null)
                payload.put("ftr", filter.getFilters());

            msg.setPayload(payload);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while constructing JSON Object - vertices.", ex);
        }

        if (this.getDebug()) {
            printDebug(direction, apiFun, payload.toString());
        }

        /* Instantiating deferred object */
        final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
        /* Extracting promise */
        Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

        this.getParentGraph().getConn().call(apiFun, msg).then(message -> {
            if (cmpType.equals("v")) {
                JSONArray vertices = new JSONArray();
                Vertex v;

                for (Iterator it = message.keys(); it.hasNext(); ) {
                    v = new Vertex();
                    v.setProperty("_source", it.next().toString());
                    vertices.put(v);
                }

                deferred.resolve(new JSONObject(vertices));
            } else {
                JSONArray edges = new JSONArray();
                Edge e;

                for (Iterator it = message.keys(); it.hasNext(); ) {
                    e = new Edge();
                    e.setSource(it.next().toString());
                    edges.put(e);
                }

                deferred.resolve(new JSONObject(edges));
            }
        }, deferred::reject);

        return promise;
    }

    Promise<JSONObject, JSONObject, Integer> inDegree(String cmp, Filter filter) {
        return degree(cmp, filter, "in");
    }

    Promise<JSONObject, JSONObject, Integer> outDegree(String cmp, Filter filter) {
        return degree(cmp, filter, "out");
    }

    Promise<JSONObject, JSONObject, Integer> degree(String cmpType, Filter filter, String direction) {
        final String apiFun = "ex_degree";

        this.validateCmp(cmpType);

        if (!this.hasId()) {
            throw new Error("Vertex ID is required.");
        }

        this.validateGraphLabel();

        Message msg = new Message();
        JSONObject payload = new JSONObject();

        try {
            payload.put("graph", this.getParentGraph().getLabel());
            payload.put("id", this.getId());
            payload.put("dir", direction);
            payload.put("cmp", cmpType.toLowerCase());

            if (filter != null)
                payload.put("ftr", filter.getFilters());

            msg.setPayload(payload);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while constructing JSON Object - vertices.", ex);
        }

        if (this.getDebug()) {
            printDebug(direction + " Degree", apiFun, payload.toString());
        }

        return this.getParentGraph().getConn().call(apiFun, msg);
    }
}
