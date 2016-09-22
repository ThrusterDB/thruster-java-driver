package org.trueno.driver.lib.core;

import org.junit.Test;
import static org.junit.Assert.*;
import org.trueno.driver.lib.core.data_structures.Filter;
import org.trueno.driver.lib.core.data_structures.Graph;

/**
 * Created by: miguel
 * Date: 9/20/16
 * Purpose:
 */

public class TruenoTest {

    private final Trueno trueno = new Trueno("http://localhost", 8000);

    @Test
    public void CountVerticesInGraph() {
        System.out.println("Connecting...");

        /* Connecting */
        trueno.connect(s -> {
            System.out.println("Connected: " + s.id());
            System.out.println("------------------------Properties, computed, and meta-------------------------------");

            /* instantiate graph */
            Graph g = trueno.Graph("graphi");

            Filter filter = g.filter().term("prop.name", "aura");

            g.count("v", filter).whenCompleteAsync((ret, err) -> {
                if (ret != null) {
                    System.out.println("Info from Graph g counted: " + ret.toString());
                } else {
                    throw new Error("Error: Could not count Graph g info " + err);
                }
            });

            g.count("v").whenCompleteAsync((ret, err) -> {
                if (ret != null) {
                    System.out.println("Total vertices in graph " + ret.toString());
                } else {
                    throw new Error("Could not count vertices " + err);
                }

            });
        }, socket -> System.out.println("disconnected"));

        //fail("Finish Implementation");
    }

    @Test
    public void CreateNewGraph() {
        System.out.println("Connecting...");

        /* Connecting */
        trueno.connect(s -> {

            System.out.println("connected " + s.id());
            System.out.println("------------------------Properties, computed, and meta-------------------------------");

            /* instantiate graph */
            Graph g = trueno.Graph("graphi");

            /* Adding properties and computed fields */
            g.setProperty("version", 1);

            g.setComputed("pagerank", "average", 2.55);
            g.setComputed("pagerank", "low", 1);


            g.create().whenCompleteAsync((ret, err) -> {
                if (ret != null) {
                    System.out.println("Graph g created " + ret.toString());
                } else {
                    throw new Error("Error: Graph g creation failed " + err);
                }
            });

        }, socket -> System.out.println("disconnected"));

        //fail("Finish Implementation");
    }

    @Test
    public void FetchGraphFromDB() {
        System.out.println("Connecting...");

        /* Connecting */
        trueno.connect(s -> {

            System.out.println("connected " + s.id());
            System.out.println("------------------------Properties, computed, and meta-------------------------------");

            /* instantiate graph */
            Graph g = trueno.Graph("graphi");

            Filter filter = g.filter()
                    .term("prop.version", 1);

            Filter filter2 = g.filter()
                    .term("prop.name", "aura");


            g.fetch("g", filter).whenComplete((result, error) -> {
                if (result != null) {
                    System.out.println("Info from Graph g component v fetched " + result.toString());
                }
                else {
                    throw new Error("Could not fetch information from graph", error);
                }
            });

            g.fetch("v", filter2).whenCompleteAsync((ret, err) -> {
                if (ret != null) {
                    System.out.println("Info from Graph g component v fetched " + ret.toString());
                }
                else {
                    throw new Error("Error: Could not fetch Graph g component v info " + err);
                }
            });

            g.fetch("e").whenCompleteAsync((ret, err) -> {
                if (ret != null) {
                    System.out.println("All edges " + ret.toString());
                }
                else {
                    throw new Error("Error: fetching all edges " + err);
                }
            });
        }, socket -> System.out.println("disconnected"));

        //fail("Finish Implementation");
    }
}