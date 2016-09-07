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
    private boolean isBulkOpen = false;
    private HashMap<String,Edge> edges;
    private HashMap<String,Vertex> vertices;

    /* invoke super constructor */
    public Graph() {
        super();

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

    public Edge[] getEdges() {
        return (Edge[])edges.values().toArray();
    }

    public Vertex[] getVertices() {
        return (Vertex[])vertices.values().toArray();
    }

    public void setConn(RPC conn) {
        this.conn = conn;
    }

    public void setBulkOpen(boolean bulkOpen) {
        isBulkOpen = bulkOpen;
    }

    /*======================== REMOTE OPERATIONS =======================*/

    public Promise create() {

        /* This object reference */
        final Graph self = this;
        final String apiFunc = "ex_create";

            /* If label is not present throw error */
        if (this.getLabel() == null) {
            throw new Error("Graph label is required");
        }
        /* Set the id */
        this.setId(this.getLabel());

        /* building the message */
        Message msg = new Message();

        try{
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
            System.out.println("DEBUG[create]: " + apiFunc +"\n"+ msg.toString());
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
}
