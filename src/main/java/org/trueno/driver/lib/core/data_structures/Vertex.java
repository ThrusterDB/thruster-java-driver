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

/**
 * <b>Vertex Class</b>
 * <p>TruenoDB Vertex primitive data structure</p>
 *
 * @author Victor Santos
 * @author Miguel Rivera
 * @version 0.1.0
 */

public class Vertex extends Component {

    private final Logger log = LoggerFactory.getLogger(Vertex.class.getSimpleName());

    /**
     * Create a Vertex instance
     */
    public Vertex() {
        this.put("partition", 0);
        this.setType("v");

        log.trace("Vertex Object created");
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
     * @param partition
     *         Partition to be set on this Vertex.
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
     * @param cmp
     *         component type for the neighbor search
     * @param filter
     *         filter for the neighbor search
     * @return Promise with the neighbors result
     */
    public Promise<JSONObject, JSONObject, Integer> in(String cmp, Filter filter) {
        return neighbors(cmp, filter, "in");
    }

    /**
     * Returns the out neighbors of this Vertex based on the supplied component type and Filter.
     *
     * @param cmp
     *         component type for the neighbor search
     * @param filter
     *         filter for the neighbor search
     * @return Promise with the neighbors result
     */
    public Promise<JSONObject, JSONObject, Integer> out(String cmp, Filter filter) {
        return neighbors(cmp, filter, "out");
    }

    /**
     * Returns the neighbors of this Vertex based on the specified component type, filter and search direction
     *
     * @param cmpType
     *         Component type for the neighbor search
     * @param filter
     *         Filter to be applied to the neighbor search
     * @param direction
     *         neighbors search direction (in, out).
     * @return Promise with the neighbors search result
     */
    public Promise<JSONObject, JSONObject, Integer> neighbors(String cmpType, Filter filter, String direction) {
        final String apiFun = "ex_neighbors";

        if (!this.validateCmp(cmpType) || !this.validateGraphLabel())
            return null;

        if (!this.hasId()) {
            log.error("Vertex ID is required.");
        }

        Message msg = new Message();
        JSONObject payload = new JSONObject();

        payload.put("graph", this.getParentGraph().getLabel());
        payload.put("id", this.getId());
        payload.put("dir", direction);
        payload.put("cmp", cmpType.toLowerCase());

        if (filter != null)
            payload.put("ftr", filter.getFilters());

        msg.setPayload(payload);

        log.debug("{} {} – {}", apiFun, direction, msg.toString());

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

    /**
     * Returns the neighbors of this Vertex based on the specified component type, filter and (in) search direction
     *
     * @param cmp
     *         Component type for the neighbor search
     * @param filter
     *         Filter to be applied to the neighbor search
     * @return Promise with the neighbors search result
     */
    Promise<JSONObject, JSONObject, Integer> inDegree(String cmp, Filter filter) {
        return degree(cmp, filter, "in");
    }

    /**
     * Returns the neighbors of this Vertex based on the specified component type, filter and (out) search direction
     *
     * @param cmp
     *         Component type for the neighbor search
     * @param filter
     *         Filter to be applied to the neighbor search
     * @return Promise with the neighbors search result
     */
    Promise<JSONObject, JSONObject, Integer> outDegree(String cmp, Filter filter) {
        return degree(cmp, filter, "out");
    }

    /**
     * Returns the neighbors of this Vertex based on the specified component type, filter and search direction
     *
     * @param cmpType
     *         Component type for the neighbor search
     * @param filter
     *         Filter to be applied to the neighbor search
     * @param direction
     *         neighbors search direction (in, out).
     * @return Promise with the neighbors search result
     */
    Promise<JSONObject, JSONObject, Integer> degree(String cmpType, Filter filter, String direction) {
        final String apiFun = "ex_degree";

        if (!this.validateCmp(cmpType) || !this.validateGraphLabel())
            return null;

        if (!this.hasId()) {
            throw new Error("Vertex ID is required.");
        }

        Message msg = new Message();
        JSONObject payload = new JSONObject();

            payload.put("graph", this.getParentGraph().getLabel());
            payload.put("id", this.getId());
            payload.put("dir", direction);
            payload.put("cmp", cmpType.toLowerCase());

            if (filter != null)
                payload.put("ftr", filter.getFilters());

            msg.setPayload(payload);

        log.debug("{} {} – {}", apiFun, direction, msg.toString());

        return this.getParentGraph().getConn().call(apiFun, msg);
    }
}
