package org.trueno.driver.lib.core.data_structures;

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
            this.put("partition", "");

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

    CompletableFuture in(String cmp, Filter filter) {
        return neighbors(cmp, filter, "in");
    }

    CompletableFuture out(String cmp, Filter filter) {
        return neighbors(cmp, filter, "out");
    }

    public CompletableFuture<JSONObject> neighbors(String cmpType, Filter filter, String direction) {
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

        return this.getParentGraph().getConn().call(apiFun, msg).whenCompleteAsync((ret, err) -> {
            if (ret != null) {
                CompletableFuture.supplyAsync(() -> {
                    if (cmpType.equals("v")) {
                        JSONArray vertices = new JSONArray();
                        Vertex v;

                        for(Iterator it = ret.keys(); it.hasNext();) {
                            v = new Vertex();
                            v.setProperty("_source", it.next().toString());
                            vertices.put(v);
                        }

                        return vertices;
                    } else {
                        JSONArray edges = new JSONArray();
                        Edge e;

                        for(Iterator it = ret.keys(); it.hasNext();) {
                            e = new Edge();
                            e.setSource(it.next().toString());
                            edges.put(e);
                        }

                        return edges;
                    }
                });
            }
            else {
                throw new Error("An error occurred while fetching RPC message - vertex neighbors", err);
            }
        });
    }

    CompletableFuture inDegree(String cmp, Filter filter) {
        return degree(cmp, filter, "in");
    }

    CompletableFuture outDegree(String cmp, Filter filter) {
        return degree(cmp, filter, "out");
    }

    CompletableFuture degree(String cmpType, Filter filter, String direction) {
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

        return this.getParentGraph().getConn().call(apiFun, msg).handleAsync((ret, err) -> {
            if (ret != null)
                return ret;
            else
                throw new Error("Error occurred while fulfilling destroy promise - vertex", err);
        });
    }
}
