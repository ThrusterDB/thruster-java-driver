package org.trueno.driver.lib.core;

import org.json.JSONObject;
import org.junit.*;

import static org.junit.Assert.*;

import org.junit.experimental.ParallelComputer;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;
import org.trueno.driver.lib.core.data_structures.Filter;
import org.trueno.driver.lib.core.data_structures.Graph;

import java.util.concurrent.TimeUnit;

/**
 * @author Miguel Rivera
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
        }
        catch (Exception ex) {
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
    public void CreateGraph() {
        System.out.println("------------------------Properties, computed, and meta-------------------------------");

        /* instantiate graph */
        Graph g = trueno.Graph("graphi");

        /* Adding properties and computed fields */
        g.setProperty("version", 1);

        g.setComputed("pagerank", "average", 2.55);
        g.setComputed("pagerank", "low", 1);

        try {
            g.create().then(message -> {
                System.out.println(message.toString());
                assertEquals("", message.toString());
            }, error -> {
                System.out.println(error.toString());
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void CountVerticesInGraph() {
        System.out.println("------------------------Properties, computed, and meta-------------------------------");

        /* instantiate graph */
        Graph g = trueno.Graph("graphi");

        Filter filter = g.filter().term("prop.name", "aura");

        try {
            g.count("v", filter).then(message -> {
                System.out.println(message.toString());
                assertEquals("", message.toString());
            }, error -> {
                System.out.println(error.toString());
            });

            g.count("v").then(message -> {
                System.out.println(message.toString());
                assertEquals("", message.toString());
            }, error -> {
                System.out.println(error.toString());
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void FetchGraphFromDB() {
        System.out.println("------------------------Properties, computed, and meta-------------------------------");

            /* instantiate graph */
        Graph g = trueno.Graph("graphi");

        Filter filter = g.filter().term("prop.version", 1);

        Filter filter2 = g.filter().term("prop.name", "aura");

        try {
            g.fetch("g", filter).then(message -> {
                System.out.println(message.toString());
                assertEquals("", message.toString());
            }, error -> {
                System.out.println(error.toString());
            });

            g.fetch("v", filter2).then(message -> {
                System.out.println(message.toString());
                assertEquals("", message.toString());
            }, error -> {
                System.out.println(error.toString());
            });

            g.fetch("e").then(message -> {
                System.out.println(message.toString());
                assertEquals("", message.toString());
            }, error -> {
                System.out.println(error.toString());
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void DeleteGraph() {
        System.out.println("------------------------Properties, computed, and meta-------------------------------");

        /* instantiate graph */
        Graph g = trueno.Graph("graphi");

        /* Adding properties and computed fields */
        g.setProperty("version", 1);

        g.setComputed("pagerank", "average", 2.55);
        g.setComputed("pagerank", "low", 1);

        try {
            g.destroy().then(message -> {
                System.out.println(message.toString());
                assertEquals("", message.toString());
            }, error -> {
                System.out.println(error.toString());
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}