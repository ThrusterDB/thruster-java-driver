package org.trueno.driver.lib.core.data_structures;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.jdeferred.Deferred;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trueno.driver.lib.core.communication.Message;

import java.util.*;
import java.util.regex.Pattern;

/**
 * <b>Component Class</b>
 * <p>Base class that provides basic functionality to the Trueno primitive data structures – Graph, Vertex and Edge</p>
 *
 * @author Victor Santos
 * @author Miguel Rivera
 * @version 0.1.0
 * @see org.json.JSONObject
 */
public class Component extends JSONObject {

    private String ref;
    private String type;
    private Graph parentGraph;

    private final Logger log = LoggerFactory.getLogger(Component.class.getName());

    /**
     * Default constructor.
     */
    public Component() {
        /* Initialize JSON properties */
        /* set UUID */
        this.ref = UUID.randomUUID().toString();

        /* setting id */
        this.put("id", "");

        /* setting Job ID */
        this.put("jobId", "");

        /* setting label */
        this.put("label", "");

        /* Setting property fields */
        this.put("prop", new JSONObject());
        /* Setting computed fields */
        this.put("comp", new JSONObject());
        /* Setting meta fields */
        this.put("meta", new JSONObject());
    }

    /**
     * Overloaded constructor. Allows to pass by parameter a Graph, Vertex or Edge already instantiated.
     *
     * @param obj JSONObject with preset keys id, prop (properties), meta and comp (computed).
     */
    public Component(JSONObject obj) {
        super();

        this.setId(obj.get("id"));
        this.setProperty(obj.getJSONObject("prop"));
        this.setMeta(obj.getJSONObject("meta"));
        this.setComputed(obj.getJSONObject("comp"));
    }

    /**
     * Returns the value associated with the reference ID of this Component.
     *
     * @return the reference ID of this Component
     */
    public String getRef() {
        return this.ref;
    }

    /**
     * Returns the value associated with the ID of this Component.
     *
     * @return the ID of this Component
     */
    public Object getId() {
        return this.get("id");
    }

    /**
     * Set the value associated with the ID of this Component.
     *
     * @param value the new ID value.
     */
    public void setId(Object value) {
        this.put("id", value);
    }

    /**
     * Returns the Job ID of this Component.
     *
     * @return Job ID of this Component
     */
    public String getJobId() {
        return this.get("jobId").toString();
    }

    /**
     * Set the Job ID of this Component.
     *
     * @param value the new Job ID.
     */
    public void setJobId(Object value) {
        this.put("jobId", value);
    }

    /**
     * Returns whether the ID property is set.
     *
     * @return true if ID is set, false otherwise.
     */
    public boolean hasId() {
        return this.has("id") && !this.get("id").toString().isEmpty();
    }

    /**
     * Returns the value associated with the label of this Component.
     *
     * @return the label of this Component
     */
    public String getLabel() {
        return this.get("label").toString();
    }

    /**
     * Set the value associated with the label of this Component.
     *
     * @param value the new label value.
     */
    public void setLabel(String value) {
        this.put("label", value);
    }

    /**
     * Returns the value associated with the type of this Component.
     *
     * @return the type of this Component (v = vertex, e = edge).
     */
    public String getType() {
        return this.type;
    }

    /**
     * Set the value associated with the type of this Component.
     *
     * @param type the new type value (v = vertex, e = edge).
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Returns the parent Graph of this Component.
     *
     * @return the parent Graph of this Component.
     */
    public Graph getParentGraph() {
        return this.parentGraph;
    }

    /**
     * Sets or updates the parent Graph of this Component.
     *
     * @param g the new parentGraph of this Component.
     */
    public void setParentGraph(Graph g) {
        this.parentGraph = g;
    }

    /**
     * Returns the properties of this Component in JSON format.
     *
     * @return the properties of this Component.
     */
    public JSONObject properties() {
        return (JSONObject) this.get("prop");
    }

    public boolean hasProperty(String prop) {
        return ((JSONObject) this.get("prop")).has(prop);
    }

    /**
     * Returns the value associated with a property of this Component.
     *
     * @param prop value of the desired property.
     * @return the value of the specified property.
     */
    public Object getProperty(String prop) {
        return ((JSONObject) this.get("prop")).get(prop);
    }

    /**
     * Sets or updates the value of a property in this Component.
     *
     * @param prop the value of the new property.
     */
    public void setProperty(JSONObject prop) {
        this.put("prop", prop);
    }

    /**
     * Sets or updates the specified property with the supplied value.
     *
     * @param prop  the property to be updated.
     * @param value the new value of the property being updated.
     */
    public void setProperty(String prop, Object value) {
        ((JSONObject) this.get("prop")).put(prop, value);
    }

    /**
     * Removes a property from the properties in this Component.
     *
     * @param prop the property to be removed.
     */
    public void removeProperty(String prop) {
        ((JSONObject) this.get("prop")).remove(prop);
    }

    /**
     * Returns the computed field of this Component in JSON format.
     *
     * @return the contents of the computed field of this Component.
     */
    public JSONObject computed() {
        return (JSONObject) this.get("comp");
    }

    /**
     * Sets the computed field of this Component.
     */
    public void setComputed(JSONObject json) {
        this.put("comp", json);
    }

    /**
     * Retrieves the computed property of this Component for a specific algorithm.
     *
     * @param algo the algorithm that contains the property.
     * @param prop the property that will be retrieved.
     */
    public Object getComputed(String algo, String prop) {
        JSONObject comp = (JSONObject) this.get("comp");

        /* if algo not present, throw error */
        if (!comp.has(algo)) {
            log.error("Provided algorithm(" + algo + ") is not present");
            return null;
        }
        if (!((JSONObject) comp.get(algo)).has(prop)) {
            log.error("Provided algorithm property(" + prop + ") is not present");
            return null;
        }

        /* return property of this algorithm */
        return ((JSONObject) comp.get(algo)).get(prop);
    }

    /**
     * Removes the computed property of this Component for a specific algorithm.
     *
     * @param algo the algorithm which will have a property removed.
     * @param prop the property that will be removed.
     */
    public boolean removeComputed(String algo, String prop) {
        JSONObject comp = (JSONObject) this.get("comp");

        /* if algo not present, throw error */
        if (!comp.has(algo)) {
            log.error("Provided algorithm (" + algo + ") is not present");
            return false;
        }
        if (!((JSONObject) comp.get(algo)).has(prop)) {
            log.error("Provided algorithm property (" + prop + ") is not present");
            return false;
        }

        ((JSONObject) comp.get(algo)).remove(prop);

        return true;
    }

    /**
     * Sets the computed property of this Component for a specific algorithm.
     *
     * @param algo the algorithm which will have its property changed.
     * @param prop the property that will be changed.
     */
    public void setComputed(String algo, String prop, Object value) {
        JSONObject comp = (JSONObject) this.get("comp");

        /* if algo property does not exist, create it */
        if (!comp.has(algo)) {
            comp.put(algo, new JSONObject());
        }

        /* Adding computed property */
        ((JSONObject) comp.get(algo)).put(prop, value);
    }

    /**
     * Returns the value of the meta property of this Component as a HashMap.
     *
     * @return a HashMap of the meta property of this component.
     */
    public HashMap meta() {
        return (HashMap) this.get("meta");
    }

    /**
     * Returns the meta property of this component.
     *
     * @return the value of the meta property.
     */
    public Object getMeta(String prop) {
        return ((JSONObject) this.get("meta")).get(prop);
    }

    /**
     * Sets the meta property of this Component
     */
    public void setMeta(JSONObject meta) {
        this.put("meta", meta);
    }

    /**
     * Verifies that the label for this Component is present.
     */
    boolean validateGraphLabel() {
        /* If label is not present throw error */
        if (this.parentGraph.getLabel().isEmpty()) {
            log.error("Graph label is required");
            return false;
        }
        return true;
    }

    /**
     * Persist this component changes in the remote database.
     *
     * @return Promise with async operation result.
     */
    public Promise<JSONObject, JSONObject, Integer> persist() {
        final String apiFun = "ex_persist";

        if (!this.validateGraphLabel()) {
            final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
            Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

            log.error("{} – Graph label not set", this.getId());
            deferred.reject(new JSONObject().put("error", this.getId() + " – Graph label not set"));
            return promise;
        }

        /* validate edge source and target */
        if (this.type.equals("e")) {
            if ((!((Edge) this).hasSource()) || (!((Edge) this).hasTarget())) {
                final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
                Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

                log.error("Edge source and target are required");
                deferred.reject(new JSONObject().put("error", this.getId() + " – Edge source and target are required"));
                return promise;
            }
        }

        Message msg = new Message();
        JSONObject payload = new JSONObject();

        payload.put("graph", this.parentGraph.getLabel());
        payload.put("type", this.type);
        payload.put("obj", this);

        msg.setPayload(payload);

        log.trace("{} – {}", apiFun, msg.toString(2));

        if (this.parentGraph.isBulkOpen()) {
            log.trace("Persisting using bulk operation");
            this.parentGraph.pushOperation(apiFun, msg.getPayload());
            return null;

        } else {
            log.trace("Persisting using Promise");
            return this.parentGraph.getConn().call(apiFun, msg).then(message -> {
                this.setId(((JSONObject) message.getJSONArray("result").get(1)).get("_id"));
            });
        }
    }

    /**
     * Destroy component(s) at the remote database.
     *
     * @param cmp Component to be destroyed.
     * @param ftr Filter to be applied.
     * @return Promise with the async destruction result.
     */
    public Promise<JSONObject, JSONObject, Integer> destroy(String cmp, Filter ftr) {
        final String apiFun = "ex_destroy";

        //Validate presence of graph label
        if (!this.validateGraphLabel()) {
            final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
            Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

            log.error("{} – Graph label not set", this.getId());
            deferred.reject(new JSONObject().put("error", this.getId() + " – Graph label not set"));
            return promise;
        }

        if (this.type.equals("g") && cmp != null) {
            if (!this.validateCmp(cmp)) {
                final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
                Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

                log.error("{} – Invalid Component", this.getId());
                deferred.reject(new JSONObject().put("error", this.getId() + " – Invalid Component"));
                return promise;
            }
        } else if (this.getId() == null) {
            final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
            Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

            log.error("{} – Component ID is required", this.getId());
            deferred.reject(new JSONObject().put("error", this.getId() + " – Component ID is required"));
            return promise;
        }

        Message msg = new Message();
        JSONObject payload = new JSONObject();

        payload.put("graph", this.getParentGraph().getLabel());

        if (this.type.equals("g")) {
            payload.put("type", cmp);
            payload.put("ftr", ftr != null ? ftr.getFilters() : new Filter().getFilters());
        } else {
            payload.put("type", this.type);
        }

        payload.put("obj", new JSONObject().put("id", this.getId()));

        msg.setPayload(payload);

        log.trace("{} – {}", apiFun, payload.toString(2));

        return this.parentGraph.getConn().call(apiFun, msg);
    }

    /**
     * Overload method for destroy.
     *
     * @param cmp Component to be destroyed.
     * @return Promise with the async destruction result.
     */
    public Promise<JSONObject, JSONObject, Integer> destroy(String cmp) {
        return this.destroy(cmp, null);
    }

    /**
     * Overload method for destroy.
     *
     * @return Promise with the async destruction result.
     */
    public Promise<JSONObject, JSONObject, Integer> destroy() {
        return this.destroy(null, null);
    }

    /**
     * Validate type of this Component.
     *
     * @param cmp this component type ('v','V', 'e','E', 'g', or 'G').
     */
    boolean validateCmp(String cmp) {
        if (!Pattern.compile("v|V|e|E|g|G").matcher(cmp).find()) {
            log.error("Component must be one of the following: 'g', 'G', v','V', 'e','E', provided value: " + cmp);
            return false;
        }
        return true;
    }
}
