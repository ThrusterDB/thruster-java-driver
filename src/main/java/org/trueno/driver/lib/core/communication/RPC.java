package org.trueno.driver.lib.core.communication;

import com.github.nkzawa.socketio.client.Ack;
import com.github.nkzawa.socketio.client.IO;
import com.github.nkzawa.socketio.client.Socket;
import org.jdeferred.Deferred;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.lang.reflect.Method;
import java.util.logging.LogManager;

/**
 * <b>RPC Class</b>
 * <p>Provides the communication facilities to interact with the Trueno Remote Database</p>
 *
 * @author Victor Santos
 * @author Miguel Rivera
 * @version 0.1.0
 */
public class RPC {

    private String host;
    private int port;
    private Socket socket;

    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Default Constructor
     */
    public RPC() {

        /* Set default properties */
        this.host = "http://localhost";
        this.port = 8000;
        this.socket = null;
    }

    /**
     * Overloaded Constructor
     *
     * @param host Hostname for database connection
     * @param port Port number for database connection
     */
    public RPC(String host, Integer port) {

        this();

        this.host = host != null ? host : this.host;
        this.port = port != null ? port : this.port;
    }

    /**
     * Invokes a function in the Remote Database specified by the supplied method name and arguments.
     *
     * @param method Method in the Trueno Remote Database to execute.
     * @param arg    Arguments for the invoked function.
     * @return Promise with async result.
     */
    public Promise<JSONObject, JSONObject, Integer> call(final String method, final JSONObject arg) {

        final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
        Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

        this.socket.emit(method, arg, (Ack) objects -> {

            JSONObject args = (JSONObject) objects[0];

            try {
                JSONObject value = new JSONObject();
                value.put("result", args.get("_payload"));

                log.trace("Server response: {}", value.toString(2));

                if (args.get("_status").toString().equals(Status.SUCCESS.toString())) {
                    deferred.resolve(value);
                } else if (args.get("_status").toString().equals(Status.ERROR.toString())) {
                    deferred.reject(value);
                }

            } catch (JSONException e) {
                throw new RuntimeException("An error occurred while manipulating JSON message", e);
            }
        });

        return promise;
    }

    /**
     * Connects to the Trueno Database
     *
     * @param connCallback Callback function to be executed if connection is successful.
     * @param discCallback Callback function to be executed if connection is unsuccessful.
     */
    public void connect(final Callback connCallback, final Callback discCallback) {
        try {
            this.socket = IO.socket(this.host + ":" + this.port);
        } catch (URISyntaxException e) {
            throw new Error("Invalid host and port specified for Trueno connection", e);
        }

        this.socket
                .on(Socket.EVENT_CONNECT, args -> connCallback.method(this.socket))
                .on(Socket.EVENT_DISCONNECT, args -> discCallback.method(this.socket));

        this.socket.connect();
    }

    public void disconnect() {
        this.socket.disconnect();
    }
}
