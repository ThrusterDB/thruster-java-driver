package org.trueno.driver.lib.core.data_structures;

import org.jdeferred.Deferred;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trueno.driver.lib.core.communication.Message;

import java.util.Iterator;


// FIXME: Standarize all promise calls; instead of returning a JSONObject should be a JSONArray  (list of Component)
// FIXME: Return of promise should be iterators and must be support streams

/**
 * <b>Vertex Class</b>
 * <p>TruenoDB Vertex primitive data structure</p>
 *
 * @author Victor Santos
 * @author Miguel Rivera
 * @author Edgardo Barsallo Yi
 * @version 0.1.0
 */
public class Vertex extends Component {

    private final Logger log = LoggerFactory.getLogger(Vertex.class.getName());

    /**
     * Create a Vertex instance
     */
    public Vertex() {
        this.put("partition", 0);
        this.setType("v");
    }

    /**
     * Overloaded constructor. Allows to pass by parameter a Graph, Vertex or Edge already instantiated.
     *
     * @param obj JSONObject with preset keys id, prop (properties), meta and comp (computed).
     */
    public Vertex(JSONObject obj) {
        super(obj);
    }

    /**
     * Gets the partition(s) of this Vertex.
     *
     * @return the partition(s).
     */
    public String getPartition() {
        return this.get("partition").toString();
    }

    /**
     * Sets a partition on this Vertex.
     *
     * @param partition Partition to be set on this Vertex.
     */
    public void setPartition(String partition) {
        this.put("partition", partition);
    }

    /**
     * Return a new filter to be applied on this Vertex.
     *
     * @return the new Filter set on this Vertex.
     */
    public Filter filter() {
        return new Filter();
    }

    /**
     * Returns the in neighbors of this Vertex based on the supplied component type and Filter.
     *
     * @param cmp    component type for the neighbor search
     * @param filter filter for the neighbor search
     * @return Promise with the neighbors result
     */
    public Promise<JSONArray, JSONObject, Integer> in(String cmp, Filter filter) {
        return neighbors(cmp, filter, "in");
    }

    /**
     * Returns the out neighbors of this Vertex based on the supplied component type and Filter.
     *
     * @param cmp    component type for the neighbor search
     * @param filter filter for the neighbor search
     * @return Promise with the neighbors result
     */
    public Promise<JSONArray, JSONObject, Integer> out(String cmp, Filter filter) {
        return neighbors(cmp, filter, "out");
    }

    /**
     * Returns the neighbors of this Vertex based on the specified component type, filter and search direction
     *
     * @param cmp       Component type for the neighbor search
     * @param filter    Filter to be applied to the neighbor search
     * @param direction neighbors search direction (in, out).
     * @return Promise with the neighbors search result
     */
    public Promise<JSONArray, JSONObject, Integer> neighbors(String cmp, Filter filter, String direction) {
        final String apiFun = "ex_neighbors";

        final Deferred<JSONArray, JSONObject, Integer> deferred = new DeferredObject<>();
        Promise<JSONArray, JSONObject, Integer> promise = deferred.promise();

        /* Validate the component */
        if (!this.validateCmp(cmp)) {
            log.error("{} – Invalid Component", this.getId());
            deferred.reject(new JSONObject().put("error", this.getId() + " – Invalid Component"));

            return promise;
        }

        /* If label is not present throw error */
        if (!this.validateGraphLabel()) {
            log.error("{} – Graph label not set", this.getId());
            deferred.reject(new JSONObject().put("error", this.getId() + " – Graph label not set"));

            return promise;
        }

        if (!this.hasId()) {
            log.error("Vertex ID is required.");
        }

        Message msg = new Message();
        JSONObject payload = new JSONObject();

        payload.put("graph", this.getParentGraph().getLabel());
        payload.put("id", this.getId());
        payload.put("dir", direction);
        payload.put("cmp", cmp.toLowerCase());

        if (filter != null)
            payload.put("ftr", filter.getFilters());

        msg.setPayload(payload);

        log.trace("{} {} – {}", apiFun, direction, msg.toString());

        this.getParentGraph().getConn().call(apiFun, msg).then(message -> {
//            System.out.println("____neighbors: (" + cmp + ") --> " + message);
            if (cmp.equals("v")) {
                JSONArray vertices = new JSONArray();
                Vertex v;

//                System.out.println("____neighbors: --> " + message.get("result"));

                // FIXME: This should be done by a method
                for (Iterator it = ((JSONArray)message.get("result")).iterator(); it.hasNext(); ) {
//                    System.out.println("____neighbors: iter --> " + ((JSONObject)it.next()).get("_source"));
                    v = new Vertex((JSONObject) ((JSONObject)it.next()).get("_source"));
//                    System.out.println("____neighbors: v --> " + v);
                    vertices.put(v);
                }


//                System.out.println("____neighbors: --> " + vertices);

                deferred.resolve(vertices);
            } else {
                JSONArray edges = new JSONArray();
                Edge e;

                for (Iterator it = ((JSONArray)message.get("result")).iterator(); it.hasNext(); ) {
                    e = new Edge((JSONObject) ((JSONObject)it.next()).get("_source"));
                    edges.put(e);
                }

                deferred.resolve(edges);
            }
        }, deferred::reject);

        return promise;
    }

    /**
     * Returns the neighbors of this Vertex based on the specified component type, filter and (in) search direction
     *
     * @param cmp    Component type for the neighbor search
     * @param filter Filter to be applied to the neighbor search
     * @return Promise with the neighbors search result
     */
    Promise<JSONObject, JSONObject, Integer> inDegree(String cmp, Filter filter) {
        return degree(cmp, filter, "in");
    }

    /**
     * Returns the neighbors of this Vertex based on the specified component type, filter and (out) search direction
     *
     * @param cmp    Component type for the neighbor search
     * @param filter Filter to be applied to the neighbor search
     * @return Promise with the neighbors search result
     */
    Promise<JSONObject, JSONObject, Integer> outDegree(String cmp, Filter filter) {
        return degree(cmp, filter, "out");
    }

    /**
     * Returns the neighbors of this Vertex based on the specified component type, filter and search direction
     *
     * @param cmp       Component type for the neighbor search
     * @param filter    Filter to be applied to the neighbor search
     * @param direction neighbors search direction (in, out).
     * @return Promise with the neighbors search result
     */
    Promise<JSONObject, JSONObject, Integer> degree(String cmp, Filter filter, String direction) {
        final String apiFun = "ex_degree";

        /* Validate the component */
        if (!this.validateCmp(cmp)) {
            final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
            Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

            log.error("{} – Invalid Component", this.getId());
            deferred.reject(new JSONObject().put("error", this.getId() + " – Invalid Component"));

            return promise;
        }

        /* If label is not present throw error */
        if (!this.validateGraphLabel()) {
            final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
            Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

            log.error("{} – Graph label not set", this.getId());
            deferred.reject(new JSONObject().put("error", this.getId() + " – Graph label not set"));

            return promise;
        }

        if (!this.hasId()) {
            final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
            Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

            log.error("Vertex ID is required.");
            deferred.reject(new JSONObject().put("error", "Vertex ID is required."));
            return promise;
        }

        Message msg = new Message();
        JSONObject payload = new JSONObject();

        payload.put("graph", this.getParentGraph().getLabel());
        payload.put("id", this.getId());
        payload.put("dir", direction);
        payload.put("cmp", cmp.toLowerCase());

        if (filter != null)
            payload.put("ftr", filter.getFilters());

        msg.setPayload(payload);

        log.trace("{} {} – {}", apiFun, direction, msg.toString());

        return this.getParentGraph().getConn().call(apiFun, msg);
    }
}
