package org.trueno.driver.lib.core;

import org.json.JSONObject;
import org.junit.*;

import static org.junit.Assert.*;

import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;
import org.trueno.driver.lib.core.data_structures.Filter;
import org.trueno.driver.lib.core.data_structures.Graph;

import java.util.concurrent.TimeUnit;

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
    public void CreateGraph() {
        System.out.println("------------------------Properties, computed, and meta-------------------------------");

        /* instantiate graph */
        Graph g = trueno.Graph("graphi");

        /* Adding properties and computed fields */
        g.setProperty("version", 1);

        g.setComputed("pagerank", "average", 2.55);
        g.setComputed("pagerank", "low", 1);

        try {
            JSONObject result = g.create().get();

            Thread.sleep(250);

            assertEquals("", result.toString());
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
            JSONObject res1 = g.count("v", filter).get();
            JSONObject res2 = g.count("v").get();

            Thread.sleep(250);

            assertEquals("", res1.toString());
            assertEquals("", res2.toString());
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
            JSONObject res1 = g.fetch("g", filter).get();
            JSONObject res2 = g.fetch("v", filter2).get();
            JSONObject res3 = g.fetch("e").get();

            Thread.sleep(250);

            assertEquals("", res1.toString());
            assertEquals("", res2.toString());
            assertEquals("", res3.toString());

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
            JSONObject result = g.destroy().get();

            Thread.sleep(250);

            assertEquals("", result.toString());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}