package org.trueno.driver.lib.core.data_structures;

import org.jdeferred.Deferred;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.json.JSONArray;
import org.json.JSONObject;
import org.trueno.driver.lib.core.communication.Message;

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

    /**
     * Create a Vertex instance
     */
    public Vertex() {
        this.put("partition", 0);
        this.setType(ComponentType.VERTEX);
    }

    /**
     * Overloaded constructor. Allows to pass by parameter a Graph, Vertex or Edge already instantiated.
     *
     * @param obj JSONObject with preset keys id, prop (properties), meta and comp (computed).
     * @param parent Graph that contains the node.
     */
    public Vertex(JSONObject obj, Graph parent) {
        super(obj);

        this.put("partition", 0);
        this.setType(ComponentType.VERTEX);
        this.setParentGraph(parent);
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
    public Promise<JSONArray, JSONObject, Integer> in(ComponentType cmp, Filter filter, Filter secondary) {
        return neighbors(cmp, filter, secondary, "in");
    }

    /**
     * Returns the out neighbors of this Vertex based on the supplied component type and Filter.
     *
     * @param cmp    component type for the neighbor search
     * @param filter filter for the neighbor search
     * @return Promise with the neighbors result
     */
    public Promise<JSONArray, JSONObject, Integer> out(ComponentType cmp, Filter filter, Filter secondary) {
        return neighbors(cmp, filter, secondary, "out");
    }

    /**
     * Returns the neighbors of this Vertex based on the specified component type, filter and search direction
     *
     * @param cmp       Component type for the neighbor search
     * @param filter    Filter to be applied to the neighbor search
     * @param direction neighbors search direction (in, out).
     * @return Promise with the neighbors search result
     */
    private Promise<JSONArray, JSONObject, Integer> neighbors(ComponentType cmp, Filter filter, Filter secondary, String direction) {
        final String apiFun = "ex_neighbors";
        final Deferred<JSONArray, JSONObject, Integer> deferred = new DeferredObject<>();
        Promise<JSONArray, JSONObject, Integer> promise = deferred.promise();

        if (!this.hasId()) {
            log.error("Vertex ID is required.");
            deferred.reject(new JSONObject().put("error", "Vertex ID is required."));
        }
        else if (cmp.invalid()) {
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

            payload.put("graph", this.getParentGraph().getLabel());
            payload.put("id", this.getId());
            payload.put("dir", direction);
            payload.put("cmp", cmp.toString());

            if (filter != null)
                payload.put("ftr", filter.getFilters());

            if (secondary != null)
                payload.put("sFtr", secondary.getFilters());

            msg.setPayload(payload);

            log.debug("{} {} – {}", apiFun, direction, msg.toString(2));

            this.getParentGraph().getConn().call(apiFun, msg).then(message -> {
                switch (cmp) {
                    case VERTEX:
                        JSONArray vertices = ComponentHelper.toVertexArray(message, this.getParentGraph());
                        deferred.resolve(vertices);
                        break;

                    case EDGE:
                        JSONArray edges = ComponentHelper.toEdgeArray(message, this.getParentGraph());
                        deferred.resolve(edges);
                        break;
                }
            }, deferred::reject);
        }

        return promise;
    }

    /**
     * Returns the neighbors of this Vertex based on the specified component type, filter and (in) search direction
     *
     * @param cmp    Component type for the neighbor search
     * @param filter Filter to be applied to the neighbor search
     * @return Promise with the neighbors search result
     */
    Promise<JSONObject, JSONObject, Integer> inDegree(ComponentType cmp, Filter filter) {
        return degree(cmp, filter, "in");
    }

    /**
     * Returns the neighbors of this Vertex based on the specified component type, filter and (out) search direction
     *
     * @param cmp    Component type for the neighbor search
     * @param filter Filter to be applied to the neighbor search
     * @return Promise with the neighbors search result
     */
    Promise<JSONObject, JSONObject, Integer> outDegree(ComponentType cmp, Filter filter) {
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
    private Promise<JSONObject, JSONObject, Integer> degree(ComponentType cmp, Filter filter, String direction) {
        final String apiFun = "ex_degree";
        final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
        Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

        if (!this.hasId()) {
            log.error("Vertex ID is required.");
            deferred.reject(new JSONObject().put("error", "Vertex ID is required."));
        }
        else if (cmp.invalid()) {
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

            payload.put("graph", this.getParentGraph().getLabel());
            payload.put("id", this.getId());
            payload.put("dir", direction);
            payload.put("cmp", cmp.toString());

            if (filter != null)
                payload.put("ftr", filter.getFilters());

            msg.setPayload(payload);

            log.debug("{} {} – {}", apiFun, direction, msg.toString(2));

            this.getParentGraph().getConn().call(apiFun, msg).then(result -> {
                deferred.resolve((JSONObject)result.get("payload"));
            });
        }

        return promise;
    }
}
