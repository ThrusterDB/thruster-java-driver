package org.trueno.driver.lib.core.data_structures;

import org.json.JSONException;
import org.json.JSONObject;
import org.trueno.driver.lib.core.communication.Message;
import org.trueno.driver.lib.core.communication.RPC;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

/**
 * Created by: victor
 * Date: 7/19/16
 * Purpose:
 */

public class Graph extends Component {

    private RPC conn;
    private ArrayList<JSONObject> bulkOperations;
    private boolean isBulkOpen = false;
    private HashMap<String, Edge> edges;
    private HashMap<String, Vertex> vertices;

    /* invoke super constructor */
    public Graph() {
        /* init bulk operations */
        this.bulkOperations = new ArrayList<>();

        /* setting supper properties */
        this.setType("g");
        this.setParentGraph(this);
        this.setDebug(false);

        /* initializing edges and vertices maps */
        this.edges = new HashMap<>();
        this.vertices = new HashMap<>();
    }

    /*======================== GETTERS & SETTERS =======================*/

    public boolean isBulkOpen() {
        return isBulkOpen;
    }

    public RPC getConn() {
        return conn;
    }

    public Edge[] edges() {
        return (Edge[]) edges.values().toArray();
    }

    public Vertex[] vertices() {
        return (Vertex[]) vertices.values().toArray();
    }

    public void setConn(RPC conn) {
        this.conn = conn;
    }

    public void setBulkOpen(boolean bulkOpen) {
        isBulkOpen = bulkOpen;
    }

    /*======================== OPERATIONS =======================*/

    /**
     * Creates a new filter to be applied to corresponding operations.
     *
     * @return The new filter instance.
     */
    public Filter filter() {
        return new Filter();
    }

    /**
     * Creates a new vertex associated with this graph.
     *
     * @return The new vertex.
     */
    public Vertex addVertex() {

        Vertex v = new Vertex();
        v.setDebug(this.getDebug());
        v.setParentGraph(this);
        /* adding vertex to the collection */
        System.out.println("v.getRef() " + v.getRef());
        this.vertices.put(v.getRef(), v);

        return v;
    }

    /**
     * Creates a new edge associated with this graph.
     *
     * @return The new edge.
     */
    public Edge addEdge() {

        Edge e = new Edge();
        e.setDebug(this.getDebug());
        e.setParentGraph(this);
        /* adding edge to the collection */
        this.edges.put(e.getRef(), e);

        return e;
    }

    /**
     * Open batch operation zone.
     */
    public void openBatch() {
        this.isBulkOpen = true;
    }

    /**
     * Submit the batch operation in bulk into the database.
     *
     * @return Promise with the bulk operations results.
     */
    public CompletableFuture<JSONObject> closeBatch() {
        return this.bulk();
    }

    /**
     * Pushes the operation string and parameter into the bulk list.
     *
     * @param op  The operation to be inserted into the bulk list.
     * @param obj The operation object.
     */
    public void pushOperation(String op, JSONObject obj) {

        JSONObject json = new JSONObject();

        try {
            json.put("op", op);
            json.put("content", obj);
        } catch (JSONException e) {
            throw new RuntimeException("Error while constructing JSON Object", e);
        }

        this.bulkOperations.add(json);
    }

    /*======================== REMOTE OPERATIONS =======================*/

    /**
     * Fetchs components from Elastic Search.
     *
     * @return The requested instantiated set of components.
     */
    public CompletableFuture<JSONObject> fetch(String cmp) {
        return this.fetch(cmp, null);
    }

    /**
     * Fetchs components from Elastic Search.
     *
     * @return The requested instantiated set of components.
     */
    public CompletableFuture<JSONObject> fetch(String cmp, Filter ftr) {

        /* Validate the component */
        this.validateCmp(cmp);

        final String apiFunc = "ex_fetch";

        /* If label is not present throw error */
        this.validateGraphLabel();

        /* building the message */
        Message msg = new Message();

        try {
            /* building payload */
            JSONObject payload = new JSONObject();

            payload.put("graph", this.getLabel());
            payload.put("type", cmp.toLowerCase());

            /* if parameter is not null */
            if (ftr != null) {
                payload.put("ftr", ftr.getFilters());
            }
            /* set the payload */
            msg.setPayload(payload);

        } catch (JSONException e) {
            throw new RuntimeException("Error while constructing JSON Object - fetch", e);
        }

        /* if debug display operation params */
        if (this.getDebug()) {
            printDebug("fetch ", apiFunc, msg.toString());
        }

        return this.conn.call(apiFunc, msg);
    }

    public CompletableFuture<JSONObject> open() {

        final String apiFunc = "ex_open";

        /* If label is not present throw error */
        this.validateGraphLabel();

        /* building the message */
        Message msg = new Message();

        try {
            /* building payload */
            JSONObject payload = new JSONObject();

            payload.put("graph", this.getLabel());
            payload.put("type", "g");
            payload.put("mask", true);
            payload.put("obj",  this);

            /* set the payload */
            msg.setPayload(payload);

        } catch (JSONException e) {
            throw new RuntimeException("Error while constructing JSON Object - fetch", e);
        }

        /* if debug display operation params */
        if (this.getDebug()) {
            printDebug("open ", apiFunc, msg.toString());
        }

        return this.conn.call(apiFunc, msg);
    }

    /**
     * Count components at the remote database.
     *
     * @param cmp The component type, can be 'v','V', 'e','E', 'g', or 'G'
     * @return Promise with the count result.
     */
    public CompletableFuture<JSONObject> count(String cmp) {
        return this.count(cmp, null);
    }

    /**
     * Count components at the remote database.
     *
     * @param cmp The component type, can be 'v','V', 'e','E', 'g', or 'G'
     * @param ftr The filter to be applied
     * @return Promise with the count result.
     */
    public CompletableFuture<JSONObject> count(String cmp, Filter ftr) {

        /* Validate the component */
        this.validateCmp(cmp);

        /* This object reference */
        final String apiFun = "ex_count";

        /* If label is not present throw error */
        this.validateGraphLabel();

        /* building the message */
        Message msg = new Message();

        try {
            /* building payload */
            JSONObject payload = new JSONObject();
            payload.put("graph", this.getLabel());
            payload.put("type", cmp.toLowerCase());

            /* if parameter is not null */
            if (ftr != null) {
                payload.put("ftr", ftr.getFilters());
            }
            /* set the payload */
            msg.setPayload(payload);

        } catch (JSONException e) {
            throw new RuntimeException("Error while constructing JSON Object - count", e);
        }

        /* if debug display operation params */
        if (this.getDebug()) {
            printDebug("count", apiFun, msg.toString());
        }

        return this.conn.call(apiFun, msg);
    }

    public CompletableFuture<JSONObject> create() {
        final String apiFunc = "ex_create";

        /* If label is not present throw error */
        this.validateGraphLabel();

        /* Set the id */
        this.setId(this.getLabel());

        /* building the message */
        Message msg = new Message();

        try {
            /* building payload */
            JSONObject payload = new JSONObject();
            payload.put("graph", this.getLabel());
            payload.put("type", this.getType());
            payload.put("obj", this);

            /* set the payload */
            msg.setPayload(payload);

        } catch (JSONException e) {
            throw new RuntimeException("Error while constructing JSON Object - create", e);
        }

        /* if debug display operation params */
        if (this.getDebug()) {
            printDebug("create", apiFunc, msg.toString());
        }

        return this.conn.call(apiFunc, msg);
    }

    public CompletableFuture<JSONObject> bulk() {
        final String apiFunc = "ex_bulk";

        /* If label is not present throw error */
        this.validateGraphLabel();

        /* building the message */
        Message msg = new Message();

        try {
            /* building payload */
            JSONObject payload = new JSONObject();
            payload.put("graph", this.getLabel());
            payload.put("operations", this.bulkOperations);
            /* set the payload */
            msg.setPayload(payload);

        } catch (JSONException e) {
            throw new RuntimeException("Error while constructing JSON Object - bulk", e);
        }

        /* if debug display operation params */
        if (this.getDebug()) {
            printDebug("bulk", apiFunc, msg.toString());
        }

        /* if no bulk operations return deferred now */
        if (this.bulkOperations.size() == 0) {

            return CompletableFuture.supplyAsync(() -> {
                JSONObject json = new JSONObject();
                try {
                    json.put("took", 0);
                    json.put("errors", false);
                    json.put("items", new ArrayList<JSONObject>());

                    return json;
                } catch (JSONException e) {
                    throw new RuntimeException("Error while constructing JSON Object - bulk", e);
                }
            });
        }

        return this.conn.call(apiFunc, msg).handleAsync((ret, err) -> {
            if (ret != null) {
                /* Clear structures */
                this.isBulkOpen = false;
                this.bulkOperations.clear();

                return ret;
            } else {
                throw new Error("Error while executing bulk operation", err);
            }
        });
    }
}
