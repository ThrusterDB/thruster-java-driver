package org.trueno.driver.lib.core;

import org.jdeferred.DoneCallback;
import org.json.JSONObject;
import org.trueno.driver.lib.core.data_structures.Filter;
import org.trueno.driver.lib.core.data_structures.Graph;
import org.trueno.driver.lib.core.data_structures.Vertex;

/**
 * For testing purpose. {@link Test} shows how to implement most of the Java driver features.
 *
 * @author Miguel Rivera
 * @author Edgardo Barsallo Yi (ebarsallo)
 */
public class Test {

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

        Filter filter = g.filter().term("prop.age", 65);

        g.fetch("v").then((result) -> {
            System.out.println("fetch 1.1: " + result);
        });


        g.fetch("v", filter).then(result -> {
            System.out.println("fetch() 2.3 -> " + result.toString());
        }).then(fn -> {
            trueno.disconnect();
        });
    }
}