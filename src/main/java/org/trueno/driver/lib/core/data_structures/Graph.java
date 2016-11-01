package org.trueno.driver.lib.core.data_structures;

import org.jdeferred.Deferred;
import org.jdeferred.DoneCallback;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.json.JSONArray;
import org.json.JSONObject;
import org.trueno.driver.lib.core.communication.Message;
import org.trueno.driver.lib.core.communication.RPC;

import java.util.ArrayList;

/**
 * <b>Graph Class</b>
 * <p>TruenoDB Graph primitive dta structure.</p>
 *
 * @author Victor Santos
 * @author Miguel Rivera
 * @version 0.1.0
 */
public class Graph extends Component {

    private RPC conn;
    private JSONArray bulkOperations;
    private boolean isBulkOpen = false;
    private ArrayList<Vertex> newVertices;
    private ArrayList<Edge> newEdges;
    private Compute compute;

    /**
     * Default constructor. Initializes a Graph structure.
     */
    public Graph(String label) {
        this.bulkOperations = new JSONArray();

        this.setId(label);
        this.setLabel(label);
        this.setType(ComponentType.GRAPH);
        this.setParentGraph(this);

        this.newEdges = new ArrayList<>();
        this.newVertices = new ArrayList<>();

        this.compute = new Compute();
        compute.setParentGraph(this);
    }

    /**
     * Returns whether the driver is sending bulk transactions to the remote Trueno DB.
     *
     * @return true if bulk is enabled, false otherwise.
     */
    public boolean isBulkOpen() {
        return isBulkOpen;
    }

    /**
     * RPC connection object of this Graph
     *
     * @return connection RPC object
     */
    public RPC getConn() {
        return conn;
    }

    /**
     * Returns an array of Edges in this Graph not yet persisted to the database
     *
     * @return array of Edges
     */
    public ArrayList<Edge> edges() {
        return newEdges;
    }

    /**
     * Returns an array of Vertices in this Graph not yet persisted to the database
     *
     * @return array of Vertices
     */
    public ArrayList<Vertex> vertices() {
        return newVertices;
    }

    /**
     * Sets the RPC connection object to be used by this Graph
     *
     * @param conn RPC connection object
     */
    public void setConn(RPC conn) {
        this.conn = conn;
    }

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
        v.setParentGraph(this);
        newVertices.add(v);

        return v;
    }

    /**
     * Creates a new edge associated with this graph.
     *
     * @return The new edge.
     */
    public Edge addEdge() {
        Edge e = new Edge();
        e.setParentGraph(this);
        newEdges.add(e);

        return e;
    }

    /**
     * Creates a new edge associated with this graph using specified source and target
     *
     * @return The new edge.
     */
    public Edge addEdge(Object source, Object target) {
        Edge e = new Edge(source, target);
        e.setParentGraph(this);
        newEdges.add(e);

        return e;
    }

    public Compute getCompute() {
        return this.compute;
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
    public Promise<JSONObject, JSONObject, Integer> closeBatch() {
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

        json.put("op", op);
        json.put("content", obj);

        this.bulkOperations.put(json);
    }

    /**
     * Fetch components from Elastic Search.
     *
     * @param cmp the component string for the search
     * @return The requested instantiated set of components.
     */
    public Promise<JSONArray, JSONObject, Integer> fetch(ComponentType cmp) {
        return this.fetch(cmp, null);
    }

    /**
     * Fetch components from Elastic Search.
     *
     * @param cmp the component string for the search
     * @param ftr Filter for the results of the search
     * @return The requested instantiated set of components.
     */
    public Promise<JSONArray, JSONObject, Integer> fetch(ComponentType cmp, Filter ftr) {
        final String apiFunc = "ex_fetch";
        final Deferred<JSONArray, JSONObject, Integer> deferred = new DeferredObject<>();
        Promise<JSONArray, JSONObject, Integer> promise = deferred.promise();

        if (cmp.invalid()) {
            log.error("{} – Invalid Component", this.getId());
            deferred.reject(new JSONObject().put("error", this.getId() + " – Invalid Component"));
        }
        else if (!this.validateGraphLabel()) {
            log.error("{} – Graph label is empty", this.getId());
            deferred.reject(new JSONObject().put("error", this.getId() + " – Graph label is empty"));
        }
        else {
            Message msg = new Message();

            JSONObject payload = new JSONObject();

            payload.put("graph", this.getLabel());
            payload.put("type", cmp.toString());

            if (ftr != null) {
                payload.put("ftr", ftr.getFilters());
            }

            msg.setPayload(payload);

            log.debug("{} – {}", apiFunc, msg.toString(2));

            this.conn.call(apiFunc, msg).then(message -> {
                switch (cmp) {
                    case GRAPH:
                        deferred.resolve(message.getJSONArray("result"));
                        break;
                    case VERTEX:
                        deferred.resolve(ComponentHelper.toVertexArray(message, this));
                        break;
                    case EDGE:
                        deferred.resolve(ComponentHelper.toEdgeArray(message, this));
                        break;
                }

            }, deferred::reject);
        }

        return promise;
    }

    /**
     * Open a Graph in the remote database.
     *
     * @return Promise with Graph result
     */
    public Promise<Graph, JSONObject, Integer> open() {
        final String apiFunc = "ex_open";
        final Deferred<Graph, JSONObject, Integer> deferred = new DeferredObject<>();
        Promise<Graph, JSONObject, Integer> promise = deferred.promise();

        if (!this.validateGraphLabel()) {
            log.error("{} – Graph label is empty", this.getId());
            deferred.reject(new JSONObject().put("error", this.getId() + " – Graph label is empty"));
        }
        else {
            Message msg = new Message();

            JSONObject payload = new JSONObject();

            payload.put("graph", this.getLabel());
            payload.put("type", ComponentType.GRAPH.toString());
            payload.put("mask", true);
            payload.put("obj", this);

            msg.setPayload(payload);

            log.debug("{} – {}", apiFunc, msg.toString(2));

            this.conn.call(apiFunc, msg).then(message -> {
                deferred.resolve(new Graph(((JSONObject)message.getJSONArray("result").get(1)).get("_source").toString()));
            }, deferred::reject);
        }

        return promise;
    }

    /**
     * Count components at the remote database.
     *
     * @param cmp The component type, can be 'v','V', 'e','E', 'g', or 'G'
     * @return Promise with the count result.
     */
    public Promise<JSONObject, JSONObject, Integer> count(ComponentType cmp) {
        return this.count(cmp, null);
    }

    /**
     * Count components at the remote database.
     *
     * @param cmp The component type, can be 'v','V', 'e','E', 'g', or 'G'
     * @param ftr The filter to be applied
     * @return Promise with the count result.
     */
    public Promise<JSONObject, JSONObject, Integer> count(ComponentType cmp, Filter ftr) {
        final String apiFun = "ex_count";

        final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
        Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

        if (cmp.invalid()) {
            log.error("{} – Invalid Component", this.getId());
            deferred.reject(new JSONObject().put("error", this.getId() + " – Invalid Component"));
        }
        else if (!this.validateGraphLabel()) {
            log.error("{} – Graph label is empty", this.getId());
            deferred.reject(new JSONObject().put("error", this.getId() + " – Graph label is empty"));
        }
        else {
            Message msg = new Message();

            JSONObject payload = new JSONObject();
            payload.put("graph", this.getLabel());
            payload.put("type", cmp.toString());

            if (ftr != null) {
                payload.put("ftr", ftr.getFilters());
            }

            msg.setPayload(payload);

            log.debug("{} – {}", apiFun, msg.toString(2));

            promise = this.conn.call(apiFun, msg).then((DoneCallback<JSONObject>) deferred::resolve, deferred::reject);
        }

        return promise;
    }

    /**
     * Create a new Graph in the remote database
     *
     * @return Promise with the creation result.
     */
    public Promise<JSONObject, JSONObject, Integer> create() {
        final String apiFun = "ex_create";

        final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
        Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

        if (!this.validateGraphLabel()) {
            log.error("{} – Graph label is empty", this.getId());
            deferred.reject(new JSONObject().put("error", this.getId() + " – Graph label is empty"));
        }
        else {
            Message msg = new Message();

            JSONObject payload = new JSONObject();
            payload.put("graph", this.getLabel());
            payload.put("type", this.getType().toString());
            payload.put("obj", this);

            msg.setPayload(payload);

            log.debug("{} – {}", apiFun, msg.toString(2));

            return this.conn.call(apiFun, msg).then(result -> {
                this.setId(((JSONObject)result.getJSONArray("result").get(1)).get("_id"));
                deferred.resolve(result);
            }, deferred::reject);
        }

        return promise;
    }

    /**
     * Perform Graph operations in batch mode
     *
     * @return Promise with the bulk operation result
     */
    private Promise<JSONObject, JSONObject, Integer> bulk() {
        final String apiFun = "ex_bulk";
        final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
        Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

        if (!this.validateGraphLabel()) {
            log.error("{} – Graph label is empty", this.getId());
            deferred.reject(new JSONObject().put("error", this.getId() + " – Graph label is empty"));
        } else {
            Message msg = new Message();

            JSONObject payload = new JSONObject();
            payload.put("graph", this.getLabel());
            payload.put("operations", this.bulkOperations);

            msg.setPayload(payload);

            log.debug("{} – {}", apiFun, msg.toString(2));

            if (this.bulkOperations.length() == 0) {
                JSONObject json = new JSONObject();

                json.put("took", 0);
                json.put("errors", false);
                json.put("items", new JSONArray());

                deferred.resolve(json);
            } else {
                this.conn.call(apiFun, msg).then((message) -> {

                    this.isBulkOpen = false;
                    this.bulkOperations = new JSONArray();

                    deferred.resolve(message);

                }, deferred::reject);
            }
        }

        return promise;
    }
}
