package org.trueno.driver.lib.core;

import org.jdeferred.DoneCallback;
import org.json.JSONObject;
import org.trueno.driver.lib.core.data_structures.Edge;
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
            g.open().then(x -> {

//                Vertex v1 = g.addVertex();
//                v1.setId("1");
//                v1.setProperty("name", "Jose");
//                System.out.println(v1.hasProperty("name"));
//                System.out.println(v1.hasProperty("age"));
//
//                Vertex v2 = g.addVertex();
//                v2.setId("2");
//                v2.setProperty("name", "Maria");
//
//                Vertex v3 = g.addVertex();
//                v3.setId("3");
//                v3.setProperty("name", "Jesus");
//
//                v1.persist();
//                v2.persist();
//                v3.persist();
//
//                Edge e1 = g.addEdge(v1.getId(), v2.getId());
//                Edge e2 = g.addEdge(v2.getId(), v3.getId());
//
//                e1.setId(1);
//                e2.setId(2);
//                e1.persist();
//                e2.persist();
//
//                g.fetch("v").then((result) -> {
//                    System.out.println("vertices: " + result);
//                });
//
//                g.fetch("e").then((result) -> {
//                    System.out.println("edges: " + result);
//                });


                // neighbors
                Vertex v1 = g.addVertex();
                v1.setId(10);
//                Filter filter = g.filter().term("label", "acquaints");
                Filter filter = g.filter().regexp("label", "knows|acquaints");
                v1.out("e", null)
                        .then((vertices) -> {
                            System.out.println("vertices --> " + vertices);
                        })
                        .fail((ex) -> {
                            System.out.println ("Something bad happened: " + ex);
                        });

                v1.in("e", null)
                        .then((edges) -> {
                            System.out.println("edges    --> " + edges);
                        })
                        .fail((ex) -> {
                            System.out.println ("Something bad happened: " + ex);
                        });
            });

        }, socket -> {
            /* disconnect */
            System.out.println("disconnected");
        });

//        Filter filter = g.filter().term("prop.age", 65);
//
//        g.fetch("v").then((result) -> {
//            System.out.println("fetch 1.1: " + result);
//        });
//
//
//        g.fetch("v", filter).then(result -> {
//            System.out.println("fetch() 2.3 -> " + result.toString());
//        }).then(fn -> {
//            trueno.disconnect();
//        });
    }
}