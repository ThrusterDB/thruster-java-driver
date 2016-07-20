package org.trueno.driver.lib.core.communication;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;

/**
 * Created by victor on 5/10/16.
 */


public class Message extends JSONObject {

    public Message() {

        try{
            /* Init meta map */
            this.put("_meta",new JSONObject());
            /* Init meta map */
            this.put("_payload", new JSONObject());
            /* Setting property fields */
            this.put("_type", "");
            /* Setting property fields */
            this.put("_status", "");


        }catch (JSONException e){
            System.out.println(e);
        }

    }

    /* Getters */
    public JSONObject getMeta() {
        try{
            return (JSONObject)this.get("_meta");
        }catch (JSONException e){
            System.out.println(e);
        }

        return null;
    }

    public JSONObject getPayload() {
        try{
            return (JSONObject)this.get("_payload");
        }catch (JSONException e){
            System.out.println(e);
        }

        return null;
    }

    public String getType() {
        try{
            return (String)this.get("_type");
        }catch (JSONException e){
            System.out.println(e);
        }

        return null;
    }

    public String getStatus() {
        try{
            return (String)this.get("_status");
        }catch (JSONException e){
            System.out.println(e);
        }
        return null;
    }


    /* Setters */
    public void setMeta(JSONObject meta) {
        try{
            this.put("_meta",meta);
        }catch (JSONException e){
            System.out.println(e);
        }
    }

    public void setPayload(JSONObject payload) {
        try{
            this.put("_payload",payload);
        }catch (JSONException e){
            System.out.println(e);
        }
    }

    public void getType(String type) {
        try{
            this.put("_type",type);
        }catch (JSONException e){
            System.out.println(e);
        }
    }

    public void setStatus(String status) {
        try{
            this.put("_status",status);
        }catch (JSONException e){
            System.out.println(e);
        }
    }

}
