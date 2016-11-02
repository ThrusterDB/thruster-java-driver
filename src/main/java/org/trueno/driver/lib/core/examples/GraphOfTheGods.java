package org.trueno.driver.lib.core.examples;

import org.trueno.driver.lib.core.Trueno;
import org.trueno.driver.lib.core.data_structures.ComponentType;
import org.trueno.driver.lib.core.data_structures.Graph;
import org.trueno.driver.lib.core.data_structures.Vertex;

import org.trueno.driver.lib.core.TruenoFactory;

/**
 * Example graph using TruenoFactory {@link TruenoFactory}. Based on roman mythology example from Titan.
 * {@link GraphOfTheGods} shows how to implement most of the features of the Java driver.
 *
 * @author Edgardo Barsallo Yi
 * @author Miguel Rivera
 */
public class GraphOfTheGods {

    public static void load(Graph graph) {

    }


    public static void neighbors(Graph graph) {

        graph.fetch(ComponentType.VERTEX, graph.filter().term("prop.name", "saturn"))
                .then(result -> {

                    Vertex saturn = new Vertex (result.getJSONObject(0), graph);
                    System.out.println(saturn);
                     /* outgoing vertices from Saturn */
                    saturn.in(ComponentType.VERTEX, null, graph.filter().regexp("label", "father"))
                            .then(vertices -> {
                                System.out.println("vertices in  <-- " + vertices);
                            });
                    /* outgoing vertices from saturn */
                    saturn.out(ComponentType.VERTEX, null, null)
                            .then(vertices -> {
                                System.out.println("vertices out --> " + vertices);
                            });

                })
                .fail(error -> {
                    System.out.println("Not found!");
                });

        graph.fetch(ComponentType.VERTEX, graph.filter().term("prop.name", "jupiter"))
                .then(result -> {

                    Vertex jupiter = new Vertex (result.getJSONObject(0), graph);
                    System.out.println(jupiter);
                     /* outgoing vertices from Saturn */
                    jupiter.in(ComponentType.EDGE, graph.filter().regexp("label", "brother"), null)
                            .then(vertices -> {
                                System.out.println("edges in  <-- " + vertices);
                            });
                    /* outgoing vertices from saturn */
                    jupiter.out(ComponentType.EDGE, graph.filter().regexp("label", "lives"), null)
                            .then(vertices -> {
                                System.out.println("edges out --> " + vertices);
                            });

                })
                .fail(error -> {
                    System.out.println("Not found!");
                });
    }


    public static void main(String[] args) {

        String database = "titan";
        Graph g = TruenoFactory.getInstance(database);
        g.open()
                .then((result) -> {

                    System.out.println(database + " instance open");
                    neighbors(g);
                })
                .fail((error)  -> {
                    System.out.println("error");
                });
        
    }
}