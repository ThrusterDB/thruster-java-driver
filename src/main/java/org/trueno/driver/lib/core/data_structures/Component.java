package org.trueno.driver.lib.core.data_structures;

import org.jdeferred.Promise;
import org.json.JSONException;
import org.json.JSONObject;
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
    private boolean debug;

    /**
     * Default constructor.
     *
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
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

        } catch (JSONException ex) {
            throw new RuntimeException("Error while manipulating JSON Object", ex);
        }
    }

    /**
     * Overloaded constructor. Allows to pass by parameter a Graph, Vertex or Edge already instantiated.
     *
     * @param obj
     *         JSONObject with preset keys id, prop (properties), meta and comp (computed).
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
    Component(JSONObject obj) {
        super();

        try {
            this.setId(obj.get("id"));
            this.setProperty(obj.getJSONObject("prop"));
            this.setMeta(obj.getJSONObject("meta"));
            this.setComputed(obj.getJSONObject("comp"));
        } catch (JSONException ex) {
            throw new RuntimeException("Error while manipulating JSON Object", ex);
        }
    }

    /**
     * Returns the value associated with the ID of this Component.
     *
     * @return the ID of this Component
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
    public Object getId() {
        try {
            return this.get("id");
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while getting the ID of a component.", ex);
        }
    }

    /**
     * Set the value associated with the ID of this Component.
     *
     * @param value
     *         the new ID value.
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
    public void setId(Object value) {
        try {
            this.put("id", value);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the ID of a component.", ex);
        }
    }

    /**
     * Returns whether the ID property is set.
     *
     * @return true if ID is set, false otherwise.
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
    public boolean hasId() {
        try {
            return this.has("id") && !this.get("id").toString().isEmpty();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while checking the ID of a component.", ex);
        }
    }

    /**
     * Returns the value associated with the label of this Component.
     *
     * @return the label of this Component
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
    public String getLabel() {
        try {
            return this.get("label").toString();
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while getting the label of a component.", ex);
        }
    }

    /**
     * Set the value associated with the label of this Component.
     *
     * @param value
     *         the new label value.
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
    public void setLabel(String value) {
        try {
            this.put("label", value);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the label of a component.", ex);
        }
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
     * @param type
     *         the new type value (v = vertex, e = edge).
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
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
     * @param g
     *         the new parentGraph of this Component.
     */
    public void setParentGraph(Graph g) {
        this.parentGraph = g;
    }

    /**
     * Returns whether debugging information for this Component is set.
     *
     * @return true if debugging information is enabled, false otherwise.
     */
    public boolean getDebug() {
        return this.debug;
    }

    /**
     * Set the output of debugging information for this Component.
     *
     * @param debug
     *         true if debugging information should be displayed, false otherwise.
     */
    public void setDebug(boolean debug) {
        this.debug = debug;
    }


    /**
     * Returns the properties of this Component in JSON format.
     *
     * @return the properties of this Component.
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
    public JSONObject properties() {
        try {
            return (JSONObject) this.get("prop");
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while retrieving the properties of a component.", ex);
        }
    }

    /**
     * Returns the value associated with a property of this Component.
     *
     * @param prop
     *         value of the desired property.
     * @return the value of the specified property.
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
    public Object getProperty(String prop) {
        try {
            return ((JSONObject) this.get("prop")).get(prop);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while getting the property of a component.", ex);
        }
    }

    /**
     * Sets or updates the value of a property in this Component.
     *
     * @param prop
     *         the value of the new property.
     * @return true if the update is successful.
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
    public boolean setProperty(JSONObject prop) {
        try {
            this.put("prop", prop);
            return true;
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the property of a component", ex);
        }
    }

    /**
     * Sets or updates the specified property with the supplied value.
     *
     * @param prop
     *         the property to be updated.
     * @param value
     *         the new value of the property being updated.
     * @return true if the update is successful.
     */
    public boolean setProperty(String prop, Object value) {
        try {
            ((JSONObject) this.get("prop")).put(prop, value);
            return true;
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the property of a component.", ex);
        }
    }

    /**
     * Removes a property from the properties in this Component.
     *
     * @param prop
     *         the property to be removed.
     * @return true if the operation was successful.
     */
    public boolean removeProperty(String prop) {
        try {
            ((JSONObject) this.get("prop")).remove(prop);
            return true;
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while removing the property of a component.", ex);
        }
    }

    /**
     * Returns the computed field of this Component in JSON format.
     *
     * @return the contents of the computed field of this Component.
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
    public JSONObject computed() {
        try {
            return (JSONObject) this.get("comp");
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while retrieving this computed field of a component.", ex);
        }
    }

    /**
     * Sets the computed field of this Component.
     *
     * @return true if the operation was successful.
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
    public boolean setComputed(JSONObject json) {
        try {
            this.put("comp", json);
            return true;
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting this computed property of a component.", ex);
        }
    }

    /**
     * Retrieves the computed property of this Component for a specific algorithm.
     *
     * @param algo
     *         the algorithm that contains the property.
     * @param prop
     *         the property that will be retrieved.
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
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
            throw new RuntimeException("An error occurred while getting this computed property of a component.", ex);
        }
    }

    /**
     * Removes the computed property of this Component for a specific algorithm.
     *
     * @param algo
     *         the algorithm which will have a property removed.
     * @param prop
     *         the property that will be removed.
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
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
            throw new RuntimeException("An error occurred while removing this computed property of a component.", ex);
        }
    }

    /**
     * Sets the computed property of this Component for a specific algorithm.
     *
     * @param algo
     *         the algorithm which will have its property changed.
     * @param prop
     *         the property that will be changed.
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
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
            throw new RuntimeException("An error occurred while setting this computed property of a component.", ex);
        }
    }


    /**
     * Returns the value of the meta property of this Component as a HashMap.
     *
     * @return a HashMap of the meta property of this component.
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
    public HashMap meta() {
        try {
            return (HashMap) this.get("meta");
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while retrieving the meta attributes of a component.", ex);
        }
    }

    /**
     * Returns the meta property of this component.
     *
     * @return the value of the meta property.
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
    public Object getMeta(String prop) {
        try {
            return ((JSONObject) this.get("meta")).get(prop);
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while getting the meta attribute of a component.", ex);
        }
    }

    /**
     * Sets the meta property of this Component
     *
     * @return true if the operation was successful.
     * @throws RuntimeException
     *         if an error occurs while retrieving or updating the value of a key.
     */
    public boolean setMeta(JSONObject json) {
        try {
            this.put("meta", json);
            return true;
        } catch (JSONException ex) {
            throw new RuntimeException("An error occurred while setting the meta property of a component.", ex);
        }
    }

    /**
     * Verifies that the label for this Component is present.
     *
     * @throws Error
     *         if no label is set.
     */
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

    /**
     * Persist this component changes in the remote database.
     *
     * @return Promise with async operation result.
     */
    public Promise<JSONObject, JSONObject, Integer> persist() {
        final String apiFun = "ex_persist";

        this.validateGraphLabel();

        /* validate edge source and target */
        if (this.type.equals("e")) {
            if ((!((Edge) this).hasSource()) || (!((Edge) this).hasTarget())) {
                throw new Error("Edge source and target are required");
            }
        }

        Message msg = new Message();
        JSONObject payload = new JSONObject();

        try {
            payload.put("graph", this.parentGraph.getLabel());
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

        return this.parentGraph.getConn().call(apiFun, msg).then(message -> {
            try {
                this.setId(((JSONObject) message.getJSONArray("result").get(1)).get("_id"));
            } catch (JSONException ex) {
                throw new RuntimeException("Error while manipulating JSON object - persist", ex);
            }
        });
    }

    /**
     * Destroy component(s) at the remote database.
     *
     * @param cmp
     *         Component to be destroyed.
     * @param ftr
     *         Filter to be applied.
     * @return Promise with the async destruction result.
     */
    public Promise<JSONObject, JSONObject, Integer> destroy(String cmp, Filter ftr) {
        final String apiFun = "ex_destroy";

        //Validate presence of graph label
        this.validateGraphLabel();

        if (this.type.equals("g") && cmp != null) {
            this.validateCmp(cmp);
        } else if (this.getId() == null) {
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
     * @param cmp
     *         Component to be destroyed.
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
     * @param cmp
     *         this component type ('v','V', 'e','E', 'g', or 'G').
     */
    void validateCmp(String cmp) {
        if (!Pattern.compile("v|V|e|E|g|G").matcher(cmp).find()) {
            throw new Error("Component must be one of the following: 'g', 'G', v','V', 'e','E', provided value: " + cmp);
        }
    }

    /**
     * Logs debug information to console and/or file.
     *
     * @param msg
     *         output message
     * @param function
     *         calling function
     * @param payload
     *         payload sent to DB
     */
    void printDebug(String msg, String function, String payload) {
        System.out.println("DEBUG[" + msg + "]: " + function + " " + payload);
    }
}
