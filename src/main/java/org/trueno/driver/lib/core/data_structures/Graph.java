package org.trueno.driver.lib.core.data_structures;

import org.jdeferred.Deferred;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trueno.driver.lib.core.communication.Message;
import org.trueno.driver.lib.core.communication.RPC;

import java.util.ArrayList;
import java.util.HashMap;

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
    private ArrayList<JSONObject> bulkOperations;
    private boolean isBulkOpen = false;
    private HashMap<String, Edge> edges;
    private HashMap<String, Vertex> vertices;

    private final Logger log = LoggerFactory.getLogger(Graph.class.getName());

    /**
     * Default constructor. Initializes a Graph structure.
     */
    public Graph() {
        /* init bulk operations */
        this.bulkOperations = new ArrayList<>();

        /* setting supper properties */
        this.setType("g");
        this.setParentGraph(this);

        /* initializing edges and vertices maps */
        this.edges = new HashMap<>();
        this.vertices = new HashMap<>();

        log.trace("Graph Object created");
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
     * Returns an array of Edges in this Graph
     *
     * @return array of Edges
     */
    public Edge[] edges() {
        return (Edge[]) edges.values().toArray();
    }

    /**
     * Returns an array of Vertices in this Graph
     *
     * @return array of Vertices
     */
    public Vertex[] vertices() {
        return (Vertex[]) vertices.values().toArray();
    }

    /**
     * Sets the RPC connection object to be used by this Graph
     *
     * @param conn
     *         RPC connection object
     */
    public void setConn(RPC conn) {
        this.conn = conn;
    }

    /**
     * Sets this Graph to do send any updates to the remote DB in bulk.
     *
     * @param bulkOpen
     *         true if bulk operations will be enabled, false if they will be disabled
     */
    public void setBulkOpen(boolean bulkOpen) {
        isBulkOpen = bulkOpen;
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
        /* adding vertex to the collection */
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
    public Promise<JSONObject, JSONObject, Integer> closeBatch() {
        return this.bulk();
    }

    /**
     * Pushes the operation string and parameter into the bulk list.
     *
     * @param op
     *         The operation to be inserted into the bulk list.
     * @param obj
     *         The operation object.
     */
    public void pushOperation(String op, JSONObject obj) {

        JSONObject json = new JSONObject();

        json.put("op", op);
        json.put("content", obj);

        this.bulkOperations.add(json);
    }

    /**
     * Fetch components from Elastic Search.
     *
     * @param cmp
     *         the component string for the search
     * @return The requested instantiated set of components.
     */
    public Promise<JSONObject, JSONObject, Integer> fetch(String cmp) {
        return this.fetch(cmp, null);
    }

    /**
     * Fetch components from Elastic Search.
     *
     * @param cmp
     *         the component string for the search
     * @param ftr
     *         Filter for the results of the search
     * @return The requested instantiated set of components.
     */
    public Promise<JSONObject, JSONObject, Integer> fetch(String cmp, Filter ftr) {
        final String apiFunc = "ex_fetch";

        /* Validate the component */
        this.validateCmp(cmp);

        /* If label is not present throw error */
        this.validateGraphLabel();

        /* building the message */
        Message msg = new Message();

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

        /* if debug display operation params */
        log.debug("{} – {}", apiFunc, msg.toString());

        return this.conn.call(apiFunc, msg);
    }

    /**
     * Open a Graph in the remote database.
     *
     * @return Promise with Graph result
     */
    public Promise<JSONObject, JSONObject, Integer> open() {

        final String apiFunc = "ex_open";

        /* If label is not present throw error */
        this.validateGraphLabel();

        /* building the message */
        Message msg = new Message();

        /* building payload */
        JSONObject payload = new JSONObject();

        payload.put("graph", this.getLabel());
        payload.put("type", "g");
        payload.put("mask", true);
        payload.put("obj", this);

        /* set the payload */
        msg.setPayload(payload);


        /* if debug display operation params */
        log.debug("{} – {}", apiFunc, msg.toString());

        return this.conn.call(apiFunc, msg);
    }

    /**
     * Count components at the remote database.
     *
     * @param cmp
     *         The component type, can be 'v','V', 'e','E', 'g', or 'G'
     * @return Promise with the count result.
     */
    public Promise<JSONObject, JSONObject, Integer> count(String cmp) {
        return this.count(cmp, null);
    }

    /**
     * Count components at the remote database.
     *
     * @param cmp
     *         The component type, can be 'v','V', 'e','E', 'g', or 'G'
     * @param ftr
     *         The filter to be applied
     * @return Promise with the count result.
     */
    public Promise<JSONObject, JSONObject, Integer> count(String cmp, Filter ftr) {

        /* Validate the component */
        this.validateCmp(cmp);

        /* This object reference */
        final String apiFun = "ex_count";

        /* If label is not present throw error */
        this.validateGraphLabel();

        /* building the message */
        Message msg = new Message();

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

        /* if debug display operation params */
        log.debug("{} – {}", apiFun, msg.toString());

        return this.conn.call(apiFun, msg);
    }

    /**
     * Create a new Graph in the remote database
     *
     * @return Promise with the creation result.
     */
    public Promise<JSONObject, JSONObject, Integer> create() {
        final String apiFunc = "ex_create";

        /* If label is not present throw error */
        this.validateGraphLabel();

        /* Set the id */
        this.setId(this.getLabel());

        /* building the message */
        Message msg = new Message();

        /* building payload */
        JSONObject payload = new JSONObject();
        payload.put("graph", this.getLabel());
        payload.put("type", this.getType());
        payload.put("obj", this);

        /* set the payload */
        msg.setPayload(payload);

        /* if debug display operation params */
        log.debug("{} – {}", apiFunc, msg.toString());

        return this.conn.call(apiFunc, msg);
    }

    /**
     * Perform Graph operations in batch mode
     *
     * @return Promise with the bulk operation result
     */
    public Promise<JSONObject, JSONObject, Integer> bulk() {
        final String apiFunc = "ex_bulk";

        /* If label is not present throw error */
        this.validateGraphLabel();

        /* building the message */
        Message msg = new Message();

        /* building payload */
        JSONObject payload = new JSONObject();
        payload.put("graph", this.getLabel());
        payload.put("operations", this.bulkOperations);
        /* set the payload */
        msg.setPayload(payload);

        /* if debug display operation params */
        log.debug("{} – {}", apiFunc, msg.toString());

        /* Instantiating deferred object */
        final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
        /* Extracting promise */
        Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

        /* if no bulk operations return deferred now */
        if (this.bulkOperations.size() == 0) {
            JSONObject json = new JSONObject();

            json.put("took", 0);
            json.put("errors", false);
            json.put("items", new ArrayList<JSONObject>());

            deferred.resolve(json);

            return promise;
        }
        else {
            this.conn.call(apiFunc, msg).then((message) -> {
            /* Clear structures */
                this.isBulkOpen = false;
                this.bulkOperations.clear();

                deferred.resolve(message);

            }, deferred::reject);
        }
        return promise;
    }
}
