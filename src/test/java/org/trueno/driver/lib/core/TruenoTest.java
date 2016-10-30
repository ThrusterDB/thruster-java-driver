package org.trueno.driver.lib.core;

import org.jdeferred.DoneCallback;
import org.jdeferred.Promise;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trueno.driver.lib.core.data_structures.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.LogManager;

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
    private int batchSize = 300;

    @BeforeClass
    public static void ConnectAndCreateGraphs() {
        try {
            System.setProperty("java.util.logging.config.file", TruenoTest.class.getClassLoader().getResource("logging.properties").getPath());
            LogManager.getLogManager().readConfiguration();
        }
        catch (IOException | NullPointerException ex) {
            LoggerFactory.getLogger(TruenoTest.class.getName()).info("Logging – Could not find TruenoDB Driver logging configuration file – Using JRE default configuration", ex);
        }

        trueno.connect(
                connSocket -> LoggerFactory.getLogger(TruenoTest.class.getName()).debug("Connected: " + connSocket.id()),
                discSocket -> LoggerFactory.getLogger(TruenoTest.class.getName()).debug("Disconnected"));

        try {
            Thread.sleep(750);

            assertTrue(trueno.isConnected());
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }

        //Create Graphs
        g1 = trueno.Graph("graphi");
        g2 = trueno.Graph("citations");

        g1.setProperty("version", 1);

        g1.setComputed("pagerank", "average", 2.55);
        g1.setComputed("pagerank", "low", 1);

        LoggerFactory.getLogger(TruenoTest.class.getName()).debug(doPromise(g1.create()));

        g2.setProperty("description", "Arxiv HEP-TH (high energy physics theory) citation graph is from the e-print arXiv and covers all the citations within a dataset of 27,770 papers with 352,807 edges");
        g2.setProperty("original-nodes", 27770);
        g2.setProperty("original-edges", 352807);

        /* Add computed fields */
        g2.setComputed("average-clustering-coefficient", "coefficient", 0.3120);
        g2.setComputed("num-of-triangles", "value", 1478735);
        g2.setComputed("fraction-of-closed-triangles", "value", 1478735);
        g2.setComputed("diameter", "value", 13);
        g2.setComputed("90-percentile-efective-diameter", "value", 5.3);

        LoggerFactory.getLogger(TruenoTest.class.getName()).debug(doPromise(g2.create()));
    }

    @AfterClass
    public static void DisconnectAndDestroyGraphs() {
        if (!trueno.isConnected()) {
            fail("Unable to connect to TruenoDB");
        }

        //Destroy Graphs
        LoggerFactory.getLogger(TruenoTest.class.getName()).debug(doPromise(g1.destroy("g", new Filter())));
        LoggerFactory.getLogger(TruenoTest.class.getName()).debug(doPromise(g2.destroy("g", new Filter())));

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

        log.debug(doPromise(v1.persist()));
        log.debug(doPromise(v2.persist()));
        log.debug(doPromise(v3.persist()));
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

        log.debug(doPromise(e1.persist()));
        log.debug(doPromise(e2.persist()));
        log.debug(doPromise(e3.persist()));
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
    public void FetchFromDB() {
        Filter filter = g1.filter().term("prop.version", 1);
        Filter filter2 = g1.filter().term("prop.name", "aura");

        log.debug(doPromiseArray(g1.fetch("g", filter)));

        log.debug(doPromiseArray(g1.fetch("v", filter2)));

        log.debug(doPromiseArray(g1.fetch("e")));
    }

    @Test
    @Category(FastTests.class)
    public void SqlQuery() {
        doPromise(trueno.sql("SELECT * FROM graphi"));
    }

    @Test
    @Category(FastTests.class)
    public void BatchOperations() {
        Vertex v1 = g1.addVertex();
        Vertex v2 = g1.addVertex();
        Vertex v3 = g1.addVertex();
        Vertex v4 = g1.addVertex();
        Vertex v5 = g1.addVertex();
        Vertex v6 = g1.addVertex();

        v1.setId(7);
        v2.setId(8);
        v3.setId(9);
        v4.setId(10);
        v5.setId(11);
        v6.setId(12);

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

        Edge e1 = g1.addEdge(7,10);   //alice -> peter
        Edge e2 = g1.addEdge(8,7);    //aura -> alice
        Edge e3 = g1.addEdge(8,9);    //aura -> alison
        Edge e4 = g1.addEdge(8,10);   //aura -> peter
        Edge e5 = g1.addEdge(9,10);   //alison -> peter
        Edge e6 = g1.addEdge(10,11);  //peter -> cat
        Edge e7 = g1.addEdge(10,12);  //peter -> bob

        e1.setLabel("knows");
        e2.setLabel("knows");
        e3.setLabel("knows");
        e4.setLabel("knows");
        e5.setLabel("knows");
        e6.setLabel("knows");
        e7.setLabel("knows");

        e1.setProperty("since", 20);
        e2.setProperty("since", 15);
        e3.setProperty("since", 25);
        e4.setProperty("since", 20);
        e5.setProperty("since", 30);
        e6.setProperty("since", 10);
        e7.setProperty("since", 20);

        g1.openBatch();

        /* Persisting vertices */
        v1.persist();
        v2.persist();
        v3.persist();
        v4.persist();
        v5.persist();
        v6.persist();

        /* Persisting edges */
        e1.persist();
        e2.persist();
        e3.persist();
        e4.persist();
        e5.persist();
        e6.persist();
        e7.persist();

        log.debug(doPromise(g1.closeBatch()));
    }

    @Test
    @Category(FastTests.class)
    @Ignore
    public void ComputePageRank() {
        Compute c = g1.getCompute();

        c.setAlgorithm(Algorithm.PAGE_RANK);
        c.setComputeParameters(new JSONObject()
                .put("schema", g1.getLabel())
                .put("TOL", "0.001")
                .put("alpha", "0.05")
                .put("persisted", "true")
                .put("persistedTable", "pr11"));

        final Boolean[] done = {false};
        final Integer[] cnt = {0};

        c.deploy().then(success -> log.debug("Job ID: " + c.getJobId()), error -> {
            log.debug("Promise error: " + error.toString());
            fail("Promise error: " + error.toString());
        });

        while (!done[0] && cnt[0] < 10) {
            c.jobStatus(c.getJobId()).then(status -> {
                log.debug("Status: " + status.toString());
                cnt[0]++;

                if (status.get("status").toString().equals(JobStatus.FINISHED.toString()) ||
                        status.get("status").toString().equals(JobStatus.ERROR.toString())) {
                    done[0] = true;
                }
            }, error -> {
                log.debug("Promise error: " + error.toString());
                fail("Promise error: " + error.toString());
                done[0] = true;
            });
        }

        assertTrue(done[0]);
        log.debug(doPromise(c.jobResult(c.getJobId())));
    }

    @Test
    @Category(FastTests.class)
    @Ignore
    public void ComputeConnectedComponent() {
        Compute c = g1.getCompute();

        c.setAlgorithm(Algorithm.CONNECTED_COMPONENTS);
        c.setComputeParameters(new JSONObject()
                .put("schema", g1.getLabel())
                .put("TOL", "false")
                .put("alpha", "0.05")
                .put("persisted", "true")
                .put("persistedTable", "pr11"));

        final Boolean[] done = {false};
        final Integer[] cnt = {0};

        c.deploy().then(success -> log.debug("Job ID: " + c.getJobId()), error -> {
            log.debug("Promise error: " + error.toString());
            fail("Promise error: " + error.toString());
        });

        while (!done[0] && cnt[0] < 10) {
            c.jobStatus(c.getJobId()).then(status -> {
                log.debug("Status: " + status.toString());
                cnt[0]++;

                if (status.get("status").toString().equals(JobStatus.FINISHED.toString()) ||
                        status.get("status").toString().equals(JobStatus.ERROR.toString())) {
                    done[0] = true;
                }
            }, error -> {
                log.debug("Promise error: " + error.toString());
                fail("Promise error: " + error.toString());
                done[0] = true;
            });
        }

        assertTrue(done[0]);
        log.debug(doPromise(c.jobResult(c.getJobId())));
    }

    @Test
    @Category(FastTests.class)
    @Ignore
    public void ComputeTriangleCount() {
        Compute c = g1.getCompute();

        c.setAlgorithm(Algorithm.TRIANGLE_COUNTING);
        c.setComputeParameters(new JSONObject()
                .put("schema", g1.getLabel())
                .put("TOL", "false")
                .put("alpha", "0.05")
                .put("persisted", "true")
                .put("persistedTable", "pr11"));

        final Boolean[] done = {false};
        final Integer[] cnt = {0};

        c.deploy().then(success -> log.debug("Job ID: " + c.getJobId()), error -> {
            log.debug("Promise error: " + error.toString());
            fail("Promise error: " + error.toString());
        });

        while (!done[0] && cnt[0] < 10) {
            c.jobStatus(c.getJobId()).then(status -> {
                log.debug("Status: " + status.toString());
                cnt[0]++;

                if (status.get("status").toString().equals(JobStatus.FINISHED.toString()) ||
                        status.get("status").toString().equals(JobStatus.ERROR.toString())) {
                    done[0] = true;
                }
            }, error -> {
                log.debug("Promise error: " + error.toString());
                fail("Promise error: " + error.toString());
                done[0] = true;
            });
        }

        assertTrue(done[0]);
        log.debug(doPromise(c.jobResult(c.getJobId())));
    }

    @Test
    @Category(FastTests.class)
    public void DeleteVertex() {
        doPromise(g1.vertices()[0].destroy());
    }

    @Test
    @Category(FastTests.class)
    public void DeleteEdge() {
        doPromise(g1.edges()[0].destroy());
    }

    @Test
    @Category(SlowTests.class)
    public void BulkCreateVertices() {
        JSONObject vertices = new JSONObject(new JSONTokener(this.getClass().getClassLoader().getResourceAsStream("citation-vertices.json")));
        int current = 0, loop = 1;

        g2.openBatch();

        for (Iterator<String> it = vertices.keys(); it.hasNext(); ) {
            Vertex v = g2.addVertex();
            v.setId(it.next());
            v.setLabel("paper");
            v.setProperty("title", vertices.get(it.next()));
            v.persist();

            current++;

            if (current == batchSize) {
                log.debug(doPromise(g2.closeBatch()));
                log.debug("Batch progress: " + current * loop + "/" + vertices.keySet().size());
                loop++;
                current = 0;
            }
        }
    }

    @Test
    @Category(SlowTests.class)
    public void BulkCreateEdges() {
        JSONArray edges = new JSONArray(new JSONTokener(this.getClass().getClassLoader().getResourceAsStream("citation-edges.json")));
        int current = 0, loop = 1;

        g2.openBatch();

        for (Iterator it = edges.iterator(); it.hasNext(); ) {
            Edge e = g2.addEdge(((JSONArray) it.next()).get(0).toString(), ((JSONArray) it.next()).get(1).toString());
            e.setId(it.next());
            e.setLabel("cited");
            e.persist();

            current++;

            if (current == batchSize) {
                log.debug(doPromise(g2.closeBatch()));
                log.debug("Batch progress: " + current * loop + "/" + edges.length());
                loop++;
                current = 0;
            }
        }
    }

    private static String doPromise(Promise<JSONObject, JSONObject, Integer> p) {
        final String[] res = new String[1];

        p.then((DoneCallback<JSONObject>) success -> res[0] = ("Promise Result:" + success.toString()),
                error -> fail((res[0] = "Promise Error:" + error.toString())));

        while (true) {
            if (!p.isPending()) {
                assertTrue(p.isResolved());
                break;
            }
        }

        return res[0];
    }

    private static String doPromiseArray(Promise<JSONArray, JSONObject, Integer> p) {
        final String[] res = new String[1];

        p.then((DoneCallback<JSONArray>) success -> res[0] = ("Promise Result:" + success.toString()),
                error -> fail((res[0] = "Promise Error:" + error.toString())));

        while (true) {
            if (!p.isPending()) {
                assertTrue(p.isResolved());
                break;
            }
        }

        return res[0];
    }
}