package org.trueno.driver.lib.core.communication;

import com.github.nkzawa.emitter.Emitter;
import com.github.nkzawa.socketio.client.Ack;
import com.github.nkzawa.socketio.client.IO;
import com.github.nkzawa.socketio.client.Socket;
import org.jdeferred.Deferred;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.json.JSONObject;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.lang.reflect.Method;

/**
 * Created by victor on 7/19/16.
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
        this.procedures = new HashMap<String, Method>();
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


    public Promise call(final String method, final JSONObject arg) {


        /* This object reference */
        final RPC self = this;

        /* Instantiating deferred object */
        final Deferred deferred = new DeferredObject();
        /* Extracting promise */
        Promise promise = deferred.promise();
        /* Sending event */
        self.socket.emit(method, arg, new Ack() {
            public void call(Object... objects) {
                deferred.resolve("done");
            }
        });

        return promise;
    }

    public void connect(final Callback connCallback, final Callback discCallback) {


        /* This object reference */
        final RPC self = this;

        /* instantiating the socket */
        try {
            this.socket = IO.socket(this.host+":"+this.port );
        } catch (URISyntaxException e) {
            System.out.println(e);
        }

        this.socket.on(Socket.EVENT_CONNECT, new Emitter.Listener() {
            public void call(Object... args) {
                connCallback.method(self.socket);
            }
        }).on(Socket.EVENT_DISCONNECT, new Emitter.Listener() {
            public void call(Object... args) {
                discCallback.method(self.socket);
            }
        });

        /* Connecting Socket */
        this.socket.connect();
    }


}
