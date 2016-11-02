package org.trueno.driver.lib.core.data_structures;

import org.json.JSONArray;
import org.json.JSONObject;
import org.trueno.driver.lib.core.data_structures.Edge;
import org.trueno.driver.lib.core.data_structures.Graph;
import org.trueno.driver.lib.core.data_structures.Vertex;

import java.util.Iterator;

/**
 * @author Edgardo Barsallo Yi (ebarsallo)
 */
public class ComponentHelper {

    public static JSONArray toVertexArray (JSONObject o, Graph graph) {
        JSONArray array = new JSONArray();

        for(Iterator it = ((JSONArray)o.get("result")).iterator(); it.hasNext(); ) {
            Object obj = it.next();
//            System.out.println("toVertexArray --> " + obj);
            Vertex v = new Vertex(toComponent(obj), graph);
            array.put(v);
        }
        return array;
    }

    public static JSONArray toEdgeArray (JSONObject o, Graph graph) {
        JSONArray array = new JSONArray();

        for(Iterator it = ((JSONArray)o.get("result")).iterator(); it.hasNext(); ) {
            Object obj = it.next();
//            System.out.println("toEdgeArray --> " + obj);
            Edge e = new Edge(toComponent(obj), graph);
            array.put(e);
        }
        return array;
    }

    public static JSONArray toGraphArray (JSONObject o) {
        JSONArray array = new JSONArray();

        for(Iterator it = ((JSONArray)o.get("result")).iterator(); it.hasNext(); ) {
            Object obj = it.next();
            Graph g = new Graph(toComponent(obj));
            array.put(g);
        }
        return array;
    }

    public static JSONObject toComponent (Object o) {
        JSONObject source = (JSONObject)o;
        JSONObject element = ((JSONObject) source.get("_source"));
        /* set identifier if not present */
        if (!element.has("id") && !source.isNull("_id")) element.put("id", source.get("_id"));
        return element;
    }
}
