package org.trueno.driver.lib.core;

import org.jdeferred.Promise;
import org.json.JSONObject;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trueno.driver.lib.core.communication.Message;
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
    private final static Trueno trueno = new Trueno("http://localhost", 8000);
    private final Logger log = LoggerFactory.getLogger(TruenoTest.class.getName());

    @BeforeClass
    public static void Connect() {
        trueno.connect(s -> LoggerFactory.getLogger(TruenoTest.class.getName()).debug("Connected: " + s.id()), s -> LoggerFactory.getLogger(TruenoTest.class.getName()).debug("Disconnected"));

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

        doPromise(g.create());
    }

    @Test
    public void CreateVertices() {
        Graph g = trueno.Graph("graphi");

        Vertex v1 = g.addVertex();
        Vertex v2 = g.addVertex();
        Vertex v3 = g.addVertex();
        Vertex v4 = g.addVertex();
        Vertex v5 = g.addVertex();
        Vertex v6 = g.addVertex();

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
    public void CreateEdges() {
        Graph g = trueno.Graph("graphi");

        Edge e1 = g.addEdge();
        Edge e2 = g.addEdge();
        Edge e3 = g.addEdge();

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
    public void CountVerticesInGraph() {
        /* instantiate graph */
        Graph g = trueno.Graph("graphi");

        Filter filter = g.filter().term("prop.name", "aura");

        doPromise(g.count("v", filter));

        doPromise(g.count("v"));
    }

    @Test
    public void FetchGraphFromDB() {
        /* instantiate graph */
        Graph g = trueno.Graph("graphi");

        Filter filter = g.filter().term("prop.version", 1);
        Filter filter2 = g.filter().term("prop.name", "aura");

        doPromise(g.fetch("g", filter));

        doPromise(g.fetch("v", filter2));

        doPromise(g.fetch("e"));
    }

    @Test
    public void DeleteGraph() {
        /* instantiate graph */
        Graph g = trueno.Graph("graphi");
        g.setId("graphi");

        doPromise(g.destroy("g", new Filter()));
    }

    private void doPromise(Promise<JSONObject, JSONObject, Integer> p) {
        p.then(message -> {
            log.debug(message.get("result").toString());
        }, error -> {
            fail(error.get("result").toString());
            log.debug(error.get("result").toString());
        });

        try {
            while (true) {
                if(!p.isPending()) {
                    assertTrue(p.isResolved());
                    break;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}