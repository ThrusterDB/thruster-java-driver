package org.trueno.driver.lib.core.data_structures;

import javafx.util.Pair;
import org.jdeferred.Promise;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by victor on 7/19/16.
 */
public class Component extends JSONObject {

    private String ref;
    private String type;
    private Graph parentGraph;
    private boolean debug;

    public Component() {

        /* Initialize JSON properties */
        try {
            /* setting id */
            this.put("id", "");
            /* setting label */
            this.put("label","");

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

        try{
            String s = this.get("id").toString();
            return (s.isEmpty())? null: s;
        } catch (JSONException e) {
            System.out.println(e);
        }

        return null;
    }

    public void setId(String value) {
        try{
            this.put("id", value);
        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    public String getLabel() {
        try{
            String s = this.get("label").toString();
            return (s.isEmpty())? null: s;
        } catch (JSONException e) {
            System.out.println(e);
        }

        return null;
    }

    public void setLabel(String value) {
        try{
            this.put("label", value);
        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    public void setType(String type){
        this.type = type;
    }

    public String getType(){
        return this.type;
    }

    public void setParentGraph(Graph g){
        this.parentGraph = g;
    }

    public Graph getParentGraph(){
        return this.parentGraph;
    }

    public void setDebug(boolean debug){
        this.debug = debug;
    }

    public boolean getDebug(){
        return this.debug;
    }


/*=========================== PROPERTIES ===========================*/

    public JSONObject properties() {

        try {
            return (JSONObject)this.get("prop");
        } catch (JSONException e) {
            System.out.println(e);
        }
        return null;
    }

    public boolean setProperty(String prop, Object value) {
        try {
            ((JSONObject)this.get("prop")).put(prop,value);
        } catch (JSONException e) {
            System.out.println(e);
            return false;
        }
        return true;
    }

    public Object getProperty(String prop) {
        try {
            return ((JSONObject) this.get("prop")).get(prop);
        } catch (JSONException e) {
            System.out.println(e);
        }
        return null;
    }

    public boolean removeProperty(String prop) {
        try {
            /* Removing the attribute */
            ((JSONObject) this.get("prop")).remove(prop);
        } catch (JSONException e) {
            System.out.println(e);
            return false;
        }
        return true;
    }

    /*============================ COMPUTED ============================*/

    public JSONObject computed() {
        try {
            return (JSONObject)this.get("comp");
        } catch (JSONException e) {
            System.out.println(e);
        }
        return null;
    }


    /* Computed collection methods */
    public boolean setComputed(String algo, String prop,  Object value) {

        try {

            JSONObject comp = (JSONObject)this.get("comp");

             /* if algo property does not exist, create it */
            if (!comp.has(algo)) {
                comp.put(algo,  new JSONObject());
            }

            /* Adding computed property */
            ((JSONObject)comp.get(algo)).put(prop, value);

            return true;

        } catch (JSONException e) {
            System.out.println(e);
        }
        return false;

    }

    public Object getComputed(String algo, String prop) {

        try {

            JSONObject comp = (JSONObject)this.get("comp");

             /* if algo not present, throw error */
            if (!comp.has(algo)) {
                throw new Error("Provided algorithm(" + algo + ") is not present");
            }
            if (!((JSONObject)comp.get(algo)).has(prop)) {
                throw new Error("Provided algorithm property(" + prop + ") is not present");
            }

            /* return property of this algorithm */
            return ((JSONObject)comp.get(algo)).get(prop);

        } catch (Exception e) {
            System.out.println(e);
        }

        return null;
    }

    public boolean removeComputed(String algo, String prop) {

        try {

            JSONObject comp = (JSONObject)this.get("comp");

             /* if algo not present, throw error */
            if (!comp.has(algo)) {
                throw new Error("Provided algorithm(" + algo + ") is not present");
            }
            if (!((JSONObject)comp.get(algo)).has(prop)) {
                throw new Error("Provided algorithm property(" + prop + ") is not present");
            }

            ((JSONObject)comp.get(algo)).remove(prop);

            return true;

        } catch (Exception e) {
            System.out.println(e);
        }

        return false;
    }
/*=========================== META ===========================*/

    public HashMap meta() {

        try {
            return (HashMap)this.get("meta");
        } catch (JSONException e) {
            System.out.println(e);
        }
        return null;
    }

    public Object getMeta(String prop) {
        try {
            return ((JSONObject) this.get("meta")).get(prop);
        } catch (JSONException e) {
            System.out.println(e);
        }
        return null;
    }

    /*======================== REMOTE OPERATIONS =======================*/

    /**
     * Persist the component changes in the remote database.
     *
     * @return
     */
    public Promise persist() {


        return null;
    }
    /**
     * Destroy component(s) at the remote database.
     *
     * @param cmp   Component to be destroyed.
     * @param ftr   Filter to be applied.
     * @return      Promise with the async destruction result.
     */
    public Promise destroy(String cmp, Filter ftr) {

        return null;
    }
    /**
     * Overload method for destroy.
     *
     * @param cmp   Component to be destroyed.
     * @return      Promise with the async destruction result.
     */
    public Promise destroy(String cmp) {
        return this.destroy(cmp, null);
    }
    /**
     * Overload method for destroy.
     *
     * @return      Promise with the async destruction result.
     */
    public Promise destroy() {
        return this.destroy(null, null);
    }



}
