package org.trueno.driver.lib.core;

import org.jdeferred.Deferred;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trueno.driver.lib.core.communication.Callback;
import org.trueno.driver.lib.core.communication.Message;
import org.trueno.driver.lib.core.communication.RPC;
import org.trueno.driver.lib.core.data_structures.Component;
import org.trueno.driver.lib.core.data_structures.Edge;
import org.trueno.driver.lib.core.data_structures.Graph;
import org.trueno.driver.lib.core.data_structures.Vertex;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.LogManager;

/**
 * TruenoDB Java Driver – Provides interaction with the Trueno Database
 *
 * @author Victor Santos
 * @author Miguel Rivera
 * @author Edgardo Barsallo Yi
 * @version 0.1.0
 */

public class Trueno {

    /* Private properties */
    private String host;
    private int port;
    private RPC rpc;
    private boolean isConnected;

    private final Logger log = LoggerFactory.getLogger(Trueno.class.getName());

    /**
     * Default Constructor. Initializes TruenoDB connection parameters to localhost:8000
     */
    public Trueno() {

        /* Set default properties */
        this.host = "http://localhost";
        this.port = 8000;
        this.isConnected = false;
        this.rpc = new RPC(this.host, this.port);

        try {
            System.setProperty("java.util.logging.config.file", this.getClass().getClassLoader().getResource("logging.properties").getPath());
            LogManager.getLogManager().readConfiguration();
        }
        catch (IOException | NullPointerException ex) {
            log.info("Logging – Could not find TruenoDB Driver logging configuration file – Using JRE default configuration", ex);
        }

        log.trace("Trueno Object created");
    }

    /**
     * Default Constructor. Initializes TruenoDB connection parameters to the specified host and port.
     *
     * @param host
     *         Hostname to initialize the TruenoDB connection.
     * @param port
     *         Port number to intialize the TruenoDB connection.
     */
    public Trueno(String host, Integer port) {

        /* calling default constructor */
        this();
        /* Set parameters */
        this.host = host != null ? host : this.host;
        this.port = port != null ? port : this.port;
        this.rpc = new RPC(this.host, this.port);

        log.trace("Overloaded Trueno Object created – Host: {} Port: {}", host, port);
    }

    /**
     * Establish the connection with the Trueno Database Server.
     *
     * @param connCallback
     *         callback function to be executed if connection is successful.
     * @param discCallback
     *         callback function to be executed if connection is unsuccessful.
     */
    public void connect(final Callback connCallback, final Callback discCallback) {
        /* Connect the rpc object */
        this.rpc.connect(socket -> {
            this.isConnected = true;
            log.trace("Trueno connected");
            connCallback.method(socket);
        }, socket -> {
            this.isConnected = false;
            log.trace("Trueno disconnected");
            discCallback.method(socket);
        });
    }

    /**
     * Disconnect from Trueno Database Server.
     */
    public void disconnect() {
        rpc.disconnect();
    }


    /**
     * Returns the hostname value in use by the driver.
     *
     * @return hostname value
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets the hostname the driver will use for connections.
     *
     * @param host
     *         hostname to be used.
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Returns the port number in use by the driver.
     *
     * @return port number.
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port number the driver will user for connections.
     *
     * @param port
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Returns the RPC object in use for communication with TruenoDB
     *
     * @return RPC object used to communicate with TruenoDB
     */
    public RPC getRpc() {
        return rpc;
    }

    /**
     * Sets the RPC object to be used for TruenoDB communications.
     *
     * @param rpc
     *         new RPC object to communicate.
     */
    public void setRpc(RPC rpc) {
        this.rpc = rpc;
    }

    /**
     * Returns whether the driver is connected to the database.
     *
     * @return true if driver is connected, false otherwise.
     */
    public boolean isConnected() {
        return isConnected;
    }

    /**
     * Creates a new graph instance related with this connection.
     *
     * @param label
     *         The graph label.
     * @return A new Graph.
     */
    public Graph Graph(String label) {

        Graph g = new Graph();
        g.setConn(this.rpc);
        g.setLabel(label);

        return g;
    }

    /**
     * Execute SQL query in the database.
     *
     * @param query
     *         The sql query to be executed.
     * @return Promise with the SQL operations results.
     */
    public Promise<JSONObject, JSONObject, Integer> sql(String query) {
        final String apiFun = "ex_sql";

        Message msg = new Message();
        JSONObject payload = new JSONObject();

        payload.put("q", query);
        msg.setPayload(payload);

        /* Instantiating deferred object */
        final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();

        /* Extracting promise */
        Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

        this.rpc.call(apiFun, msg).then(message -> {
            JSONArray vertices = new JSONArray();
            JSONArray edges = new JSONArray();

            for (Iterator it = message.keys(); it.hasNext(); ) {
                switch (((Component) it).getType()) {
                    case "v":
                        Vertex v = new Vertex();
                        v.setId(((Component) it).getId());
                        vertices.put(v);
                        break;
                    case "e":
                        Edge e = new Edge();
                        e.setId(((Component) it).getId());
                        edges.put(e);
                        break;
                }
            }

            deferred.resolve(new JSONObject().put("v", vertices).put("e", edges));
        }, deferred::reject);

        return promise;
    }
}