package org.trueno.driver.lib.core;

import org.jdeferred.Deferred;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.json.JSONArray;
import org.json.JSONObject;
import org.trueno.driver.lib.core.communication.Callback;
import org.trueno.driver.lib.core.communication.Message;
import org.trueno.driver.lib.core.communication.RPC;
import org.trueno.driver.lib.core.data_structures.*;

/**
 * TruenoDB Java Driver – Provides interaction with the Trueno Database
 *
 * @author Victor Santos
 * @author Miguel Rivera
 * @author Edgardo Barsallo Yi
 * @version 0.1.0
 */

public class Trueno {
    private String host;
    private int port;
    private RPC rpc;
    private boolean isConnected;

    public static final String DEFAULT_HOST = "http://localhost";
    public static final int DEFAULT_PORT = 8000;

    /**
     * Default Constructor. Initializes TruenoDB connection parameters to localhost:8000
     */
    public Trueno() {
        this.host = DEFAULT_HOST;
        this.port = DEFAULT_PORT;
        this.isConnected = false;
        this.rpc = new RPC(this.host, this.port);
    }

    /**
     * Default Constructor. Initializes TruenoDB connection parameters to the specified host and port.
     *
     * @param host Hostname to initialize the TruenoDB connection.
     * @param port Port number to intialize the TruenoDB connection.
     */
    public Trueno(String host, Integer port) {
        this();

        this.host = host != null ? host : this.host;
        this.port = port != null ? port : this.port;
        this.rpc = new RPC(this.host, this.port);
    }

    /**
     * Establish the connection with the Trueno Database Server.
     *
     * @param connCallback callback function to be executed if connection is successful.
     * @param discCallback callback function to be executed if connection is unsuccessful.
     */
    public Trueno connect(final Callback connCallback, final Callback discCallback) {
        this.rpc.connect(
                socket -> {
                    this.isConnected = true;
                    connCallback.method(socket);
                },
                socket -> {
                    this.isConnected = false;
                    discCallback.method(socket);
                }
        );

        return this;
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
     * @param host hostname to be used.
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
     * @param rpc new RPC object to communicate.
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
     * @param label The graph label.
     * @return A new Graph.
     */
    public Graph Graph(String label) {
        Graph g = new Graph(label);
        g.setConn(this.rpc);

        return g;
    }

    /**
     * Execute SQL query in the database.
     *
     * @param query The sql query to be executed.
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

            for (Object o : message.getJSONArray("result")) {
                JSONObject current = (JSONObject) o;
                switch ((ComponentType) current.get("_type")) {
                    case VERTEX:
                        Vertex v = new Vertex();
                        v.setId(current.get("_id"));
                        vertices.put(v);
                        break;
                    case EDGE:
                        Edge e = new Edge();
                        e.setId(current);
                        edges.put(e);
                        break;
                }
            }

            deferred.resolve(new JSONObject().put("v", vertices).put("e", edges));
        }, deferred::reject);

        return promise;
    }
}