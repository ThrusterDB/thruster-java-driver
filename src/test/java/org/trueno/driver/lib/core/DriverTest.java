package org.trueno.driver.lib.core;

import org.jdeferred.DoneCallback;
import org.json.JSONObject;
import org.trueno.driver.lib.core.data_structures.Filter;
import org.trueno.driver.lib.core.data_structures.Graph;
import org.trueno.driver.lib.core.data_structures.Vertex;

/**
 * For testing purpose. {@link DriverTest} shows how to implement most of the Java driver features.
 *
 * @author Miguel Rivera
 * @author Edgardo Barsallo Yi (ebarsallo)
 */
public class DriverTest {

    public static void main(String[] args) {

        System.out.println("Testing java-driver...");
        Trueno trueno = new Trueno("http://localhost", 8000);
        Graph g = trueno.Graph("graphi");

        trueno.connect(socket -> {
            /* connect */
            System.out.println("connected " + socket.id());

        }, socket -> {
            /* disconnect */
            System.out.println("disconnected");
        });

//        Filter filter1 = g.filter().term("prop.age", 65);
//
//        g.fetch("v", filter1).then((result) -> {
//            System.out.println("fetch() 1 --> " + result);
//        });
//
//        g.fetch("v").then(result -> {
//            System.out.println("fetch() 2 -> " + result.toString());
//        });
//
//        Filter filter2 = g.filter().term("id", 5);
//        g.fetch("v", filter2).then(result -> {
//            System.out.println("fetch() 3 -> " + result.toString());
//        });

        Vertex v1 = g.addVertex();
        v1.setId("6");
        v1.setLabel("none");
        v1.persist().then(result -> {
            System.out.println("good!");
        }).fail(e -> {
            System.out.println(e);
        });

    }

}
