package org.trueno.driver.lib.core;

import org.jdeferred.Promise;
import org.json.JSONObject;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.trueno.driver.lib.core.data_structures.Filter;
import org.trueno.driver.lib.core.data_structures.Graph;

import static org.junit.Assert.*;

/**
 * @author Miguel Rivera
 *         Date: 9/20/16
 *         Purpose:
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
    public static void Disconnect() {
        if (!trueno.isConnected()) {
            fail("Trueno Database could not connect");
        }

        trueno.disconnect();
    }

    @Test
    public void CreateGraph() {
        /* instantiate graph */
        Graph g = trueno.Graph("graphi");

        /* Adding properties and computed fields */
        g.setProperty("version", 1);

        g.setComputed("pagerank", "average", 2.55);
        g.setComputed("pagerank", "low", 1);

        doPromise(g.create(), "");
    }

    @Test
    public void CountVerticesInGraph() {
        /* instantiate graph */
        Graph g = trueno.Graph("graphi");

        Filter filter = g.filter().term("prop.name", "aura");

        doPromise(g.count("v", filter), "");

        doPromise(g.count("v"), "");
    }

    @Test
    public void FetchGraphFromDB() {
        /* instantiate graph */
        Graph g = trueno.Graph("graphi");

        Filter filter = g.filter().term("prop.version", 1);
        Filter filter2 = g.filter().term("prop.name", "aura");

        doPromise(g.fetch("g", filter), "");

        doPromise(g.fetch("v", filter2), "");

        doPromise(g.fetch("e"), "");
    }

    @Test
    public void DeleteGraph() {
        /* instantiate graph */
        Graph g = trueno.Graph("graphi");

        doPromise(g.destroy("v", new Filter()), "");
    }

    private void doPromise(Promise<JSONObject, JSONObject, Integer> p, String assertion) {
        final String[] retMsg = new String[1];

        p.then(message -> {
            retMsg[0] = message.toString();
        }, error -> fail(error.toString()));

        try {
            Thread.sleep(500);

            assertTrue(p.isResolved());

            assertEquals(assertion, retMsg[0]);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}