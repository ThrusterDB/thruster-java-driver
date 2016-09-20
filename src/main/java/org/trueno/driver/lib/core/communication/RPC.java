package org.trueno.driver.lib.core.communication;

import com.github.nkzawa.socketio.client.Ack;
import com.github.nkzawa.socketio.client.IO;
import com.github.nkzawa.socketio.client.Socket;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

/**
 * Created by: victor
 * Date: 7/19/16
 * Purpose:
 */

public class RPC {

    /* Private properties */
    private String host;
    private int port;
    private HashMap<String, Method> procedures;
    private Socket socket;

    /* Default Constructor */
    public RPC() {

        /* Set default properties */
        this.host = "http://localhost";
        this.port = 8000;
        this.procedures = new HashMap<>();
        this.socket = null;
    }

    /* Constructor with Parameters */
    public RPC(String host, Integer port) {

        /* calling default constructor */
        this();
        /* Set parameters */
        this.host = host != null ? host : this.host;
        this.port = port != null ? port : this.port;
    }

    /* Public methods */
    public void expose(String procedureName, Method procedureFunction) {
        /* Insert the procedure in the collection */
        this.procedures.put(procedureName, procedureFunction);
    }

    public CompletableFuture<JSONObject> call(final String method, final JSONObject arg) {
        return CompletableFuture.supplyAsync(() -> {
            JSONObject value = new JSONObject();

            /* Sending event */
            this.socket.emit(method, arg, (Ack) objects -> {

            /* casting json object */
                JSONObject args = (JSONObject) objects[0];

            /* checking the result and resolving or rejecting */
                try {
                    value.put("result", args.get("_payload"));

                    if (args.get("_status").toString().equals(Status.ERROR.toString())) {
                        throw new Error("An error occurred while retrieving data: " + value);
                    }

                } catch (JSONException e) {
                    throw new RuntimeException("An error occurred while manipulating JSON message", e);
                }
            });

            return value;
        });
    }

    public void connect(final Callback connCallback, final Callback discCallback) {
        /* instantiating the socket */
        try {
            this.socket = IO.socket(this.host + ":" + this.port);
        } catch (URISyntaxException e) {
            throw new Error("Could not connect to the database", e);
        }

        this.socket.on(Socket.EVENT_CONNECT, args -> connCallback.method(this.socket)).on(Socket.EVENT_DISCONNECT, args -> discCallback.method(this.socket));

        /* Connecting Socket */
        this.socket.connect();
    }
}
