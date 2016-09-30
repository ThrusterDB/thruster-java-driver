package org.trueno.driver.lib.core;

import org.json.JSONException;
import org.json.JSONObject;
import org.trueno.driver.lib.core.communication.Callback;
import org.trueno.driver.lib.core.communication.Message;
import org.trueno.driver.lib.core.communication.RPC;
import org.trueno.driver.lib.core.data_structures.Graph;

import java.util.concurrent.CompletableFuture;

/**
 * @author Victor Santos
 * @author Miguel Rivera
 *
 * Date: 7/19/16
 * Purpose:
 */

public class Trueno {

    /* Private properties */
    private boolean debug;
    private String host;
    private int port;
    private RPC rpc;
    private boolean isConnected;


    /* Default Constructor */
    public Trueno() {

        /* Set default properties */
        this.host = "http://localhost";
        this.port = 8000;
        this.isConnected = false;
        this.rpc = new RPC(this.host, this.port);

    }

    /* Constructor with Parameters */
    public Trueno(String host, Integer port) {

        /* calling default constructor */
        this();
        /* Set parameters */
        this.host = host != null ? host : this.host;
        this.port = port != null ? port : this.port;
        this.rpc = new RPC(this.host, this.port);
    }

    public void connect(final Callback connCallback, final Callback discCallback) {
        /* Connect the rpc object */
        this.rpc.connect(socket -> {
            this.isConnected = true;
            connCallback.method(socket);
        }, socket -> {
            this.isConnected = false;
            discCallback.method(socket);
        });
    }

    public void disconnect() {
        rpc.disconnect();
    }

    /*======================== GETTERS & SETTERS =======================*/

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public RPC getRpc() {
        return rpc;
    }

    public void setRpc(RPC rpc) {
        this.rpc = rpc;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void setConnected(boolean connected) {
        isConnected = connected;
    }

    /*=========================== MIX METHODS ==========================*/

    /**
     * Creates a new graph instance related with this connection.
     *
     * @param label The graph label.
     * @return A new Graph.
     */
    public Graph Graph(String label) {

        Graph g = new Graph();
        g.setConn(this.rpc);
        g.setDebug(this.debug);
        g.setLabel(label);

        return g;
    }

    /*======================== REMOTE OPERATIONS =======================*/

    /**
     * Execute SQL query in the backend.
     *
     * @param query The sql query to be executed in the backend.
     * @return Promise with the SQL operations results.
     */
    public CompletableFuture<JSONObject> sql(String query) {
        final String apiFun = "ex_sql";

        Message msg = new Message();
        JSONObject payload = new JSONObject();

        try {
            payload.put("q", query);
            msg.setPayload(payload);

        } catch (JSONException ex) {
            throw new RuntimeException("Error ocurred while manipulating JSON Object - query", ex);
        }

//        return this.rpc.call(apiFun, msg).handleAsync(() -> {
//
//        });

        return null;
    }

}