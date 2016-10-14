package org.trueno.driver.lib.core;

import org.jdeferred.Promise;
import org.json.JSONObject;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trueno.driver.lib.core.data_structures.Edge;
import org.trueno.driver.lib.core.data_structures.Filter;
import org.trueno.driver.lib.core.data_structures.Graph;
import org.trueno.driver.lib.core.data_structures.Vertex;

import static org.junit.Assert.*;

/**
 * TruenoDB Test suite – Test connection, creation of Graphs, Graph operations.
 *
 * @author Miguel Rivera
 * @version 0.1.0
 */
@FixMethodOrder(MethodSorters.JVM)
public class TruenoTest {
    private final static Trueno trueno = new Trueno();
    private final Logger log = LoggerFactory.getLogger(TruenoTest.class.getName());
    private static Graph g1;
    private static Graph g2;

    @BeforeClass
    public static void ConnectAndCreateGraphs() {
        trueno.connect(s -> LoggerFactory.getLogger(TruenoTest.class.getName()).debug("Connected: " + s.id()), s -> LoggerFactory.getLogger(TruenoTest.class.getName()).debug("Disconnected"));

        try {
            Thread.sleep(750);

            assertTrue(trueno.isConnected());
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        //Create Graphs
        g1 = trueno.Graph("graphi");
        g2 = trueno.Graph("citations");

        g1.setProperty("version", 1);

        g1.setComputed("pagerank", "average", 2.55);
        g1.setComputed("pagerank", "low", 1);

        LoggerFactory.getLogger(TruenoTest.class.getName()).debug(doPromise(g1.create()));
    }

    @AfterClass
    public static void DisconnectAndDestroyGraphs() {
        if (!trueno.isConnected()) {
            fail("Trueno Database could not connect");
        }

        //Destroy Graphs
        LoggerFactory.getLogger(TruenoTest.class.getName()).debug(doPromise(g1.destroy("g", new Filter())));

        trueno.disconnect();
    }

    @Test
    @Category(FastTests.class)
    public void CreateVertices() {
        Vertex v1 = g1.addVertex();
        Vertex v2 = g1.addVertex();
        Vertex v3 = g1.addVertex();
        Vertex v4 = g1.addVertex();
        Vertex v5 = g1.addVertex();
        Vertex v6 = g1.addVertex();

        v1.setId(1);
        v2.setId(2);
        v3.setId(3);
        v4.setId(4);
        v5.setId(5);
        v6.setId(6);

        v1.setProperty("name", "alice");
        v1.setProperty("age", "25");

        v2.setProperty("name", "aura");
        v2.setProperty("age", "30");

        v3.setProperty("name", "alison");
        v3.setProperty("age", "35");

        v4.setProperty("name", "peter");
        v4.setProperty("age", "20");

        v5.setProperty("name", "cat");
        v5.setProperty("age", "65");

        v6.setProperty("name", "bob");
        v6.setProperty("age", "50");

        v2.setProperty("name", "juan");
        v2.setComputed("pagerank", "rank", 5);
        v3.setProperty("name", "Rick");

        doPromise(v1.persist());
        doPromise(v2.persist());
        doPromise(v3.persist());
    }

    @Test
    @Category(FastTests.class)
    public void CreateEdges() {
        Edge e1 = g1.addEdge();
        Edge e2 = g1.addEdge();
        Edge e3 = g1.addEdge();

        e1.setSource("1");
        e1.setTarget("2");

        e2.setSource("2");
        e2.setTarget("3");

        e3.setSource("1");
        e3.setTarget("3");

        e1.setId(1);
        e2.setId(2);
        e3.setId(3);

        e1.setProperty("weight", 35);
        e1.setProperty("relation", "love");
        e1.setProperty("relation", "hate");
        e2.setComputed("pagerank", "rank", 5);
        e2.setProperty("weight", 45);
        e3.setProperty("relation", "joy");
        e3.setProperty("weight", 20);

        doPromise(e1.persist());
        doPromise(e2.persist());
        doPromise(e3.persist());
    }

    @Test
    @Category(FastTests.class)
    public void CountVerticesInGraph() {
        Filter filter = g1.filter().term("prop.name", "aura");

        log.debug(doPromise(g1.count("v", filter)));

        log.debug(doPromise(g1.count("v")));
    }

    @Test
    @Category(FastTests.class)
    public void FetchGraphFromDB() {
        Filter filter = g1.filter().term("prop.version", 1);
        Filter filter2 = g1.filter().term("prop.name", "aura");

        log.debug(doPromise(g1.fetch("g", filter)));

        log.debug(doPromise(g1.fetch("v", filter2)));

        log.debug(doPromise(g1.fetch("e")));
    }

    @Test
    @Category(FastTests.class)
    public void DeleteVertices() {

    }

    @Test
    @Category(FastTests.class)
    public void DeleteEdges() {

    }

    private static String doPromise(Promise<JSONObject, JSONObject, Integer> p) {
        final String[] res = new String[1];

        p.then(success -> res[0] = "Promise Result:" + success.get("result").toString(), error -> {
            if (error.keySet().size() > 1)
                fail(res[0] = ("Promise Error:" + (((JSONObject) error.get("result")).toString(4))));
            else
                fail(res[0] = "Promise Error:" + error.get("result").toString());
        });

        try {
            while (true) {
                if (!p.isPending()) {
                    assertTrue(p.isResolved());
                    break;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return res[0];
    }
}