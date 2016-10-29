package org.trueno.driver.lib.core.utils;

import org.json.JSONArray;
import org.json.JSONObject;
import org.trueno.driver.lib.core.data_structures.Edge;
import org.trueno.driver.lib.core.data_structures.Vertex;

import java.util.Iterator;

/**
 * Created by ebarsallo on 10/28/16.
 */
public class ComponentHelper {

    public static JSONArray toVertexArray (JSONObject o) {
        JSONArray array = new JSONArray();

        for(Iterator it = ((JSONArray)o.get("result")).iterator(); it.hasNext(); ) {
            Vertex v = new Vertex(toComponent(it.next()));
            array.put(v);
        }
        return array;
    }

    public static JSONArray toEdgeArray (JSONObject o) {
        JSONArray array = new JSONArray();

        for(Iterator it = ((JSONArray)o.get("result")).iterator(); it.hasNext(); ) {
            Edge e = new Edge(toComponent(it.next()));
            array.put(e);
        }
        return array;
    }

    public static JSONObject toComponent (Object o) {
        JSONObject source = (JSONObject)o;
        JSONObject element = ((JSONObject) source.get("_source"));
        /* set identifier if not present */
        if (element.isNull("id") && !source.isNull("_id")) element.put("id", source.get("_id"));
        return element;

    }
}
