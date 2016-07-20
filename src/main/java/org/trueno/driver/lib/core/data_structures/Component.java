package org.trueno.driver.lib.core.data_structures;

import javafx.util.Pair;
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


    public Component() {

        try {
            /* Setting internal fields */
            this.put("_internal", new HashMap());
            ((JSONObject) this.get("_internal")).put("modified", new HashMap());
            ((JSONObject) this.get("_internal")).put("fields", new HashMap());

            /* Setting property fields */
            this.put("_property", new HashMap());
            ((JSONObject) this.get("_property")).put("_id", "");
            ((JSONObject) this.get("_property")).put("_graphid", "");

            /* Set the attributes, computed, and meta fields */
            ((JSONObject) this.get("_property")).put("_attributes", new HashMap());
            ((JSONObject) this.get("_property")).put("_computed", new HashMap<String, HashMap>());
            ((JSONObject) this.get("_property")).put("_meta", new HashMap());


        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    /* Getters */
    public String getGraphid() {
        try {
            return (String) ((JSONObject) this.get("_property")).get("_graphid");
        } catch (JSONException e) {
            System.out.println(e);
        }
        return null;
    }

    public String getId() {
        try {
            return (String) ((JSONObject) this.get("_property")).get("_id");
        } catch (JSONException e) {
            System.out.println(e);
        }
        return null;
    }

    public HashMap getAttributes() {

        try {
            return (HashMap) ((JSONObject) this.get("_property")).get("_attributes");
        } catch (JSONException e) {
            System.out.println(e);
        }
        return null;

    }

    public HashMap getComputed() {
        try {
            return (HashMap) ((JSONObject) this.get("_property")).get("_computed");
        } catch (JSONException e) {
            System.out.println(e);
        }
        return null;
    }

    public HashMap getMeta() {
        try {
            return (HashMap) ((JSONObject) this.get("_property")).get("_meta");
        } catch (JSONException e) {
            System.out.println(e);
        }
        return null;
    }

    public HashMap getFields() {
        try {
            return (HashMap) ((JSONObject) this.get("_internal")).get("fields");
        } catch (JSONException e) {
            System.out.println(e);
        }
        return null;
    }

    /* Setters */
    public void setGraphid(String graphid) {
        try {
           ((JSONObject) this.get("_property")).put("_graphid",graphid);
        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    public void setId(String id) {
        try {
            ((JSONObject) this.get("_property")).put("_id", id);
        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    public void setAttributes(HashMap attributes) {

        try {
           ((JSONObject) this.get("_property")).put("_attributes",attributes);
        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    public void getComputed(HashMap computed) {
        try {
            ((JSONObject) this.get("_property")).put("_computed",computed);
        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    public void setMeta(HashMap meta) {
        try {
           ((JSONObject) this.get("_property")).put("_meta", meta);
        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    public void setFields(HashMap fields) {
        try {
            ((JSONObject) this.get("_internal")).put("fields",fields);
        } catch (JSONException e) {
            System.out.println(e);
        }
    }

     /*====================== ATTRIBUTES ======================*/

    /* Attributes collection methods */
    public void setAttribute(String attr, Object value) {

        try {
            this.mark("_attributes");
            /* Adding the attribute */
            ((JSONObject)((JSONObject) this.get("_property")).get("_attributes")).put(attr,value);

        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    public Object getAttribute(String attr) {

        try {
            /* returning the attribute */
           return  ((HashMap)((JSONObject) this.get("_property")).get("_attributes")).get(attr);

        } catch (JSONException e) {
            System.out.println(e);
        }

        return null;
    }

    public void removeAttribute(String attr) {

        try {
            /* Removing the attribute */
            ((HashMap)((JSONObject) this.get("_property")).get("_attributes")).remove(attr);

        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    /*====================== COMPUTED ======================*/

    /* Attributes collection methods */
    public void setComputedAlgorithm(String algo) {

        try {
            /* Marking as modified */
            this.mark("_computed");
            /* if algo attribute exist */
            if(((HashMap)((JSONObject) this.get("_property")).get("_computed")).containsKey(algo)){
                throw new Exception("Provided algorithm(" + algo + ") is already present");
            }
            /* adding the computed algorithm */
            ((HashMap)((JSONObject) this.get("_property")).get("_computed")).put(algo,new HashMap());

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public HashMap getComputedAlgorithm(String algo) {

        try {
            /* returning the attribute */
            return  (HashMap)((HashMap)((JSONObject) this.get("_property")).get("_computed")).get(algo);

        } catch (JSONException e) {
            System.out.println(e);
        }

        return null;
    }

    public void removeComputedAlgorithm(String algo) {

        try {
            /* if algo attribute exist */
            if(!((HashMap)((JSONObject) this.get("_property")).get("_computed")).containsKey(algo)){
                throw new Exception("Provided algorithm(" + algo + ") is not present");
            }
            /* adding the computed algorithm */
            ((HashMap)((JSONObject) this.get("_property")).get("_computed")).remove(algo);

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    /* Attributes collection methods */
    public void setComputedAttribute(String algo, String attr, Object value) {

        try {
            /* Marking as modified */
            this.mark("_computed");
            /* if algo attribute exist */
            if(!((HashMap)((JSONObject) this.get("_property")).get("_computed")).containsKey(algo)){
                /* adding the computed algorithm */
                ((HashMap)((JSONObject) this.get("_property")).get("_computed")).put(algo,new HashMap());
            }
            /* Setting attribute */
            ((HashMap)((HashMap)((JSONObject) this.get("_property")).get("_computed")).get(algo)).put(attr,value);

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public HashMap getComputedAttribute(String algo, String attr) {

        try {
            /* if algo attribute exist */
            if(!((HashMap)((JSONObject) this.get("_property")).get("_computed")).containsKey(algo)){
                throw new Exception("Provided algorithm(" + algo + ") is not present");
            }

            /* returning algorithm's computed attribute */
            ((HashMap)((HashMap)((JSONObject) this.get("_property")).get("_computed")).get(algo)).get(attr);

        } catch (Exception e) {
            System.out.println(e);
        }

        return null;
    }

    public void removeComputedAlgorithm(String algo, String attr) {

        try {
            /* if algo attribute exist */
            if(!((HashMap)((JSONObject) this.get("_property")).get("_computed")).containsKey(algo)){
                throw new Exception("Provided algorithm(" + algo + ") is not present");
            }
            /* removing algorithm's computed attribute */
            ((HashMap)((HashMap)((JSONObject) this.get("_property")).get("_computed")).get(algo)).remove(attr);

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    /*====================== META ======================*/

    /* Attributes collection methods */
    public void setMetaAttribute(String attr, Object value) {

        try {
            this.mark("_meta");
            /* Adding the attribute */
            ((HashMap)((JSONObject) this.get("_property")).get("_meta")).put(attr,value);

        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    public Object getMetaAttribute(String attr) {

        try {
            /* returning the attribute */
            return  ((HashMap)((JSONObject) this.get("_property")).get("_meta")).get(attr);

        } catch (JSONException e) {
            System.out.println(e);
        }

        return null;
    }

    public void removeMetaAttribute(String attr) {

        try {
            /* Removing the attribute */
            ((HashMap)((JSONObject) this.get("_property")).get("_meta")).remove(attr);

        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    /*====================== OPERATIONS ======================*/

    public void clear() {
        try {
            ((JSONObject) this.get("_internal")).put("modified", new HashMap());
        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    public void mark(String id){
        try {
            ((JSONObject)((JSONObject) this.get("_internal")).get("modified")).put(id,0);
        } catch (JSONException e) {
            System.out.println(e);
        }
    }

    public void setMapping(String id, HashMap map) {
        try {
            ((HashMap)((JSONObject) this.get("_internal")).get("fields")).put(id,map);
        } catch (JSONException e) {
            System.out.println(e);
        }
    }


    public ArrayList<Pair<String,HashMap>> flush() {

        try {
            HashMap modified = ((HashMap)((JSONObject) this.get("_internal")).get("modified"));
            ArrayList<Pair<String, HashMap>> pairs =  new ArrayList<Pair<String, HashMap>>();

            Iterator it = modified.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();
                String key = (String)pair.getKey();
                HashMap x = (HashMap)pair.getValue();

                if(x != null){
                    pairs.add(new Pair<String, HashMap>(key,x));
                }
            }

            this.clear();
            return pairs;

        } catch (JSONException e) {
            System.out.println(e);
        }

        return null;
    }

}
