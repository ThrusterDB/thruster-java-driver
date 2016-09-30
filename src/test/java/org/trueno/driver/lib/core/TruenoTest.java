package org.trueno.driver.lib.core;

import org.junit.*;

import static org.junit.Assert.*;

import org.junit.runners.MethodSorters;
import org.trueno.driver.lib.core.data_structures.Filter;
import org.trueno.driver.lib.core.data_structures.Graph;

/**
 * Created by: miguel
 * Date: 9/20/16
 * Purpose:
 */

@FixMethodOrder(MethodSorters.JVM)
public class TruenoTest {

    private final static Trueno trueno = new Trueno("http://localhost", 8000);

    @BeforeClass
    public static void Connect() {
        trueno.connect(s -> System.out.println("Connected: " + s.id()), s -> System.out.println("Disconnected"));

        try {
            Thread.sleep(750);
            assertTrue(trueno.isConnected());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @AfterClass
    public static void DisconnectDatabase() {
        if (!trueno.isConnected()) {
            fail("Trueno Database could not connect");
        }

        trueno.disconnect();
    }

    @Test
    public void CreateNewGraph() {
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
    }

    @Test
    public void CountVerticesInGraph() {
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
    }

    @Test
    public void FetchGraphFromDB() {
        System.out.println("------------------------Properties, computed, and meta-------------------------------");

            /* instantiate graph */
        Graph g = trueno.Graph("graphi");

        Filter filter = g.filter()
                .term("prop.version", 1);

        Filter filter2 = g.filter()
                .term("prop.name", "aura");


        g.fetch("g", filter).whenCompleteAsync((ret, error) -> {
            if (ret != null) {
                System.out.println("Info from Graph g component v fetched " + ret.toString());
            } else {
                throw new Error("Could not fetch information from graph", error);
            }
        });

        g.fetch("v", filter2).whenCompleteAsync((ret, err) -> {
            if (ret != null) {
                System.out.println("Info from Graph g component v fetched " + ret.toString());
            } else {
                throw new Error("Error: Could not fetch Graph g component v info " + err);
            }
        });

        g.fetch("e").whenCompleteAsync((ret, err) -> {
            if (ret != null) {
                System.out.println("All edges " + ret.toString());
            } else {
                throw new Error("Error: fetching all edges " + err);
            }
        });

    }
}