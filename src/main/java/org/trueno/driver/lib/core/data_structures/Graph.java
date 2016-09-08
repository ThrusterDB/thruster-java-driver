package org.trueno.driver.lib.core.data_structures;

import org.jdeferred.Deferred;
import org.jdeferred.DoneCallback;
import org.jdeferred.FailCallback;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.json.JSONException;
import org.json.JSONObject;
import org.trueno.driver.lib.core.Trueno;
import org.trueno.driver.lib.core.communication.Message;
import org.trueno.driver.lib.core.communication.RPC;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by victor on 7/19/16.
 */
public class Graph extends Component {

    private RPC conn;
    private ArrayList<JSONObject> bulkOperations;
    private boolean isBulkOpen = false;
    private HashMap<String, Edge> edges;
    private HashMap<String, Vertex> vertices;

    /* invoke super constructor */
    public Graph() {
        super();

        /* init bulk operations */
        this.bulkOperations = new ArrayList<JSONObject>();
        /* setting supper properties */
        this.setType("g");
        this.setParentGraph(this);
        this.setDebug(false);

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
     * @return The new filter instance.
     */
    public Filter filter() {
        return new Filter();
    }

    /**
     * Creates a new vertex associated with this graph.
     * @return  The new vertex.
     */
    public Vertex addVertex() {

        Vertex v = new Vertex();
        v.setDebug(this.getDebug());
        v.setParentGraph(this);
        /* adding vertex to the collection */
        this.vertices.put(v.getRef(),v);

        return v;
    }

    /**
     * Creates a new edge associated with this graph.
     * @return The new edge.
     */
    public Edge addEdge() {

        Edge e = new Edge();
        e.setDebug(this.getDebug());
        e.setParentGraph(this);
        /* adding edge to the collection */
        this.edges.put(e.getRef(),e);

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
     * @return Promise with the bulk operations results.
     */
    public Promise closeBatch() {
        return this.bulk();
    }

    /**
     * Pushes the operation string and parameter into the bulk list.
     * @param op   The operation to be inserted into the bulk list.
     * @param obj  The operation object.
     */
    public void pushOperation(String op, JSONObject obj) {

        JSONObject json = new JSONObject();

        try {
            json.put("op", op);
            json.put("content", obj);
        } catch (JSONException e) {
            e.printStackTrace();
        }

        this.bulkOperations.add(json);
    }

    /*======================== REMOTE OPERATIONS =======================*/
    /**
     * Fetchs components from Elastic Search.
     * @return The requested instantiated set of components.
     */
    public Promise fetch(String cmp){
        return this.fetch(cmp, null);
    }
    /**
     * Fetchs components from Elastic Search.
     * @return The requested instantiated set of components.
     */
    public Promise fetch(String cmp, Filter ftr) {

        /* Validate the component */
        this.validateCmp(cmp);

        /* This object reference */
        final Graph self = this;
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
            System.out.println(e);
        }

        /* if debug display operation params */
        if (this.getDebug()) {
            System.out.println("DEBUG[fetch]: " + apiFunc + "\n" + msg.toString());
        }

        /* Instantiating deferred object */
        final Deferred deferred = new DeferredObject();
        /* Extracting promise */
        Promise promise = deferred.promise();

        this.conn.call(apiFunc, msg).then(new DoneCallback() {
            public void onDone(Object o) {
                try {
                    JSONObject msg = (JSONObject) o;
                    deferred.resolve(msg);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        }, new FailCallback() {
            public void onFail(Object o) {
                try {
                    JSONObject msg = (JSONObject) o;
                    deferred.reject(msg);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        });

        return promise;
    }

    /**
     * Count components at the remote database.
     *
     * @param cmp The component type, can be 'v','V', 'e','E', 'g', or 'G'
     * @return Promise with the count result.
     */
    public Promise count(String cmp) {
        return this.count(cmp, null);
    }

    /**
     * Count components at the remote database.
     *
     * @param cmp The component type, can be 'v','V', 'e','E', 'g', or 'G'
     * @param ftr The filter to be applied
     * @return Promise with the count result.
     */
    public Promise count(String cmp, Filter ftr) {

        /* Validate the component */
        this.validateCmp(cmp);

        /* This object reference */
        final Graph self = this;
        final String apiFunc = "ex_count";

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
            System.out.println(e);
        }

        /* if debug display operation params */
        if (this.getDebug()) {
            System.out.println("DEBUG[count]: " + apiFunc + "\n" + msg.toString());
        }

        /* Instantiating deferred object */
        final Deferred deferred = new DeferredObject();
        /* Extracting promise */
        Promise promise = deferred.promise();

        this.conn.call(apiFunc, msg).then(new DoneCallback() {
            public void onDone(Object o) {
                try {
                    JSONObject msg = (JSONObject) o;
                    deferred.resolve(msg);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        }, new FailCallback() {
            public void onFail(Object o) {
                try {
                    JSONObject msg = (JSONObject) o;
                    deferred.reject(msg);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        });

        return promise;
    }

    public Promise create() {

        /* This object reference */
        final Graph self = this;
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
            System.out.println(e);
        }

        /* if debug display operation params */
        if (this.getDebug()) {
            System.out.println("DEBUG[create]: " + apiFunc + "\n" + msg.toString());
        }

        /* Instantiating deferred object */
        final Deferred deferred = new DeferredObject();
        /* Extracting promise */
        Promise promise = deferred.promise();

        this.conn.call(apiFunc, msg).then(new DoneCallback() {
            public void onDone(Object o) {
                try {
                    JSONObject msg = (JSONObject) o;
                    deferred.resolve(msg);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        }, new FailCallback() {
            public void onFail(Object o) {
                try {
                    JSONObject msg = (JSONObject) o;
                    deferred.reject(msg);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        });

        return promise;
    }

    public Promise bulk() {

        /* This object reference */
        final Graph self = this;
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
            System.out.println(e);
        }

        /* if debug display operation params */
        if (this.getDebug()) {
            System.out.println("DEBUG[bulk]: " + apiFunc + "\n" + msg.toString());
        }

        /* Instantiating deferred object */
        final Deferred deferred = new DeferredObject();
        /* Extracting promise */
        Promise promise = deferred.promise();

        /* if no bulk operations return deferred now */
        if(this.bulkOperations.size() == 0){

            JSONObject json = new JSONObject();
            try {
                json.put("took", 0);
                json.put("errors", false);
                json.put("items", new ArrayList<JSONObject>());
                /* resolve now */
                deferred.resolve(json);
            } catch (Exception e) {
                System.out.println(e);
            }
            return promise;
        }

        this.conn.call(apiFunc, msg).then(new DoneCallback() {
            public void onDone(Object o) {
                try {
                    /* Clear structures */
                    self.isBulkOpen = false;
                    self.bulkOperations.clear();
                    /*  resolve promise */
                    JSONObject msg = (JSONObject) o;
                    deferred.resolve(msg);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        }, new FailCallback() {
            public void onFail(Object o) {
                try {
                    JSONObject msg = (JSONObject) o;
                    deferred.reject(msg);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        });

        return promise;
    }


}
