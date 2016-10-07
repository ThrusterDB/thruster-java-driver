package org.trueno.driver.lib.core.data_structures;

import org.jdeferred.Deferred;
import org.jdeferred.DoneCallback;
import org.jdeferred.FailCallback;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.json.JSONException;
import org.json.JSONObject;
import org.trueno.driver.lib.core.communication.Message;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

/**
 * Created by: victor, miguel
 * Date: 7/19/16
 * Purpose: Create Component base class for vertices and edges
 */

public class Component extends JSONObject {

    private String ref;
    private String type;
    private Graph parentGraph;
    private boolean debug;

    Component() {

        /* set UUID */
        this.ref = UUID.randomUUID().toString();

        /* Initialize JSON properties */
        try {
            //Initially, no debugging
            debug = false;

            /* setting id */
            this.put("id", "");

            /* setting label */
            this.put("label", "");

            /* Setting property fields */
            this.put("prop", new JSONObject());
            /* Setting computed fields */
            this.put("comp", new JSONObject());
            /* Setting meta fields */
            this.put("meta", new JSONObject());

        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    /*======================== GETTERS & SETTERS =======================*/

    public String getId() {
        try {
            return this.get("id").toString();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while getting the ID of a component.", ex);
        }
    }

    public void setId(String value) {
        try {
            this.put("id", value);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the ID of a component.", ex);
        }
    }

    public boolean hasId() {
        try {
            return this.has("id") && !this.get("id").toString().isEmpty();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while checking the ID of a component.", ex);
        }
    }

    public String getLabel() {
        try {
            return this.get("label").toString();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while getting the label of a component.", ex);
        }
    }

    public void setLabel(String value) {
        try {
            this.put("label", value);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the label of a component.", ex);
        }
    }

    public String getRef() {
        return this.ref;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    public void setParentGraph(Graph g) {
        this.parentGraph = g;
    }

    public Graph getParentGraph() {
        return this.parentGraph;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public boolean getDebug() {
        return this.debug;
    }

    /*=========================== PROPERTIES ===========================*/

    public JSONObject properties() {

        try {
            return (JSONObject) this.get("prop");
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while retrieving the properties of a component.", ex);
        }
    }

    public boolean setProperty(String prop, Object value) {
        try {
            ((JSONObject) this.get("prop")).put(prop, value);
            return true;
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the property of a component.", ex);
        }
    }

    public Object getProperty(String prop) {
        try {
            return ((JSONObject) this.get("prop")).get(prop);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while getting the property of a component.", ex);
        }
    }

    public boolean removeProperty(String prop) {
        try {
            ((JSONObject) this.get("prop")).remove(prop);
            return true;
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while removing the property of a component.", ex);
        }
    }

    /*============================ COMPUTED ============================*/

    public JSONObject computed() {
        try {
            return (JSONObject) this.get("comp");
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while retrieving the computed field of a component.", ex);
        }
    }

    /* Computed collection methods */
    public boolean setComputed(String algo, String prop, Object value) {

        try {
            JSONObject comp = (JSONObject) this.get("comp");

             /* if algo property does not exist, create it */
            if (!comp.has(algo)) {
                comp.put(algo, new JSONObject());
            }

            /* Adding computed property */
            ((JSONObject) comp.get(algo)).put(prop, value);

            return true;

        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the computed property of a component.", ex);
        }
    }

    public Object getComputed(String algo, String prop) {

        try {
            JSONObject comp = (JSONObject) this.get("comp");

             /* if algo not present, throw error */
            if (!comp.has(algo)) {
                throw new Error("Provided algorithm(" + algo + ") is not present");
            }
            if (!((JSONObject) comp.get(algo)).has(prop)) {
                throw new Error("Provided algorithm property(" + prop + ") is not present");
            }

            /* return property of this algorithm */
            return ((JSONObject) comp.get(algo)).get(prop);

        } catch (Exception ex) {
            throw new RuntimeException("An error occurred while getting the computed property of a component.", ex);
        }
    }

    public boolean removeComputed(String algo, String prop) {
        try {
            JSONObject comp = (JSONObject) this.get("comp");

             /* if algo not present, throw error */
            if (!comp.has(algo)) {
                throw new Error("Provided algorithm(" + algo + ") is not present");
            }
            if (!((JSONObject) comp.get(algo)).has(prop)) {
                throw new Error("Provided algorithm property(" + prop + ") is not present");
            }

            ((JSONObject) comp.get(algo)).remove(prop);

            return true;

        } catch (Exception ex) {
            throw new RuntimeException("An error occurred while removing the computed property of a component.", ex);
        }
    }

    /*=========================== META ===========================*/

    public HashMap meta() {
        try {
            return (HashMap) this.get("meta");
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while retrieving the meta attributes of a component.", ex);
        }
    }

    public Object getMeta(String prop) {
        try {
            return ((JSONObject) this.get("meta")).get(prop);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while getting the meta attribute of a component.", ex);
        }
    }

    /*=========================== VALIDATION ===========================*/

    void validateGraphLabel() {
        /* If label is not present throw error */
        if (this.parentGraph.getLabel().isEmpty()) {
            throw new Error("Graph label is required");
        }
        /* If label is not present throw error */
        if (this.type.equals("g")) {
            this.setId(this.getLabel());
        }
    }
    /*======================== REMOTE OPERATIONS =======================*/

    /**
     * Persist the component changes in the remote database.
     *
     * @return
     */
    public Promise<JSONObject, JSONObject, Integer> persist() {
        final String apiFun = "ex_persist";

        this.validateGraphLabel();

        /* validate edge source and target */
        if (this.type.equals("e") && (!((Edge) this).hasSource()) || !(((Edge) this).hasTarget())) {
            throw new Error("Edge source and target are required");
        }

        Message msg = new Message();
        JSONObject payload = new JSONObject();

        try {
            payload.put("graph", this.getLabel());
            payload.put("type", this.type);
            payload.put("obj", this);

            msg.setPayload(payload);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while constructing JSON Object - destroy.", ex);
        }

        if (this.debug) {
            printDebug("persist", apiFun, msg.toString());
        }

        if (this.parentGraph.isBulkOpen()) {
            this.parentGraph.pushOperation(apiFun, msg.getPayload());

        }

        /* Instantiating deferred object */
        final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
        /* Extracting promise */
        Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

        this.parentGraph.getConn().call(apiFun, msg).then((message) -> {
            try {
                this.setId(message.get("id").toString());
                deferred.resolve(message);
            } catch (JSONException ex) {
                throw new RuntimeException("Error while manipulating JSON object - persist", ex);
            }
        }, deferred::reject);

        return promise;
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
        this.validateGraphLabel();

        if (this.type.equals("g") && cmp != null) {
            this.validateCmp(cmp);
        } else if ((cmp = this.getId()).isEmpty()) {
            throw new Error("Component ID is required");
        }

        Message msg = new Message();
        JSONObject payload = new JSONObject();

        try {
            payload.put("graph", this.getLabel());

            if (this.type.equals("g")) {
                payload.put("type", cmp);
                payload.put("ftr", ftr != null ? ftr : new Filter().getFilters());
            } else {
                payload.put("type", this.type);
                payload.put("obj", new JSONObject().put("id", this.getId()));
            }

            msg.setPayload(payload);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while constructing JSON Object - destroy.", ex);
        }

        if (this.debug) {
            printDebug("destroy", apiFun, payload.toString());
        }

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
     * Validate component.
     *
     * @param cmp {string} [cmp] - The component type, can be 'v','V', 'e','E', 'g', or 'G'
     */
    void validateCmp(String cmp) {
        if (!Pattern.compile("v|V|e|E|g|G").matcher(cmp).find()) {
            throw new Error("Component must be one of the following: 'g', 'G', v','V', 'e','E', provided value: " + cmp);
        }
    }

    void printDebug(String msg, String function, String payload) {
        System.out.println("DEBUG[" + msg + "]: " + function + " " + payload);
    }
}
