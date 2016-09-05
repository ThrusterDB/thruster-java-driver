package org.trueno.driver.lib.core;


import com.github.nkzawa.engineio.client.Socket;
import org.jdeferred.Deferred;
import org.jdeferred.DoneCallback;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.trueno.driver.lib.core.communication.Callback;
import org.trueno.driver.lib.core.communication.Message;
import org.trueno.driver.lib.core.communication.RPC;
import org.trueno.driver.lib.core.data_structures.Component;
import org.trueno.driver.lib.core.data_structures.Edge;
import org.trueno.driver.lib.core.data_structures.Graph;
import org.trueno.driver.lib.core.data_structures.Vertex;

/**
 * Created by victor on 7/19/16.
 */
public class Trueno {

    /* Private properties */
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
        /* This object reference */
        final Trueno self = this;

        /* Connect the rpc object */
        this.rpc.connect(new Callback() {
            public void method(com.github.nkzawa.socketio.client.Socket socket) {
                self.isConnected = true;
                connCallback.method(socket);
            }
        }, new Callback() {
            public void method(com.github.nkzawa.socketio.client.Socket socket) {
                self.isConnected = false;
                discCallback.method(socket);
            }
        });
    }

    /*================================ GRAPH EXTERNAL API METHODS ================================*/

    public Promise createGraph(Graph g) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(g);

        /* return promise with the async operation */
        return this.rpc.call("ex_createGraph", msg);

    }

    public Promise updateGraph(Graph g) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(g);

        /* return promise with the async operation */
        return this.rpc.call("ex_updateGraph", msg);

    }

    public Promise deleteGraph(Graph g) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(g);

        /* return promise with the async operation */
        return this.rpc.call("ex_deleteGraph", msg);

    }

    public Promise getGraph(Graph g) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(g);


        /* return promise with the async operation */
        return this.rpc.call("ex_getGraph", msg);

    }

    public Promise getGraphList(Graph g) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(g);

        /* return promise with the async operation */
        return this.rpc.call("ex_getGraphList", msg);

//        return new Promise((resolve, reject) => {
//        this._rpc.call('ex_getGraphList', msg).then(msg => {
//                let gl = [];
//        if (msg._payload.code == 0) {
//            let list = msg._payload.result;
//            for (var i = 0; i < list.length; i++) {
//                let g = new Graph();
//                utils.datatoComponent(list[i], g);
//                gl.push(g);
//            }
//            resolve(gl);
//        }
//      }).catch(e => {
//                reject(e);
//      });
//    });

//        /* Instantiating deferred object */
//        final Deferred deferred = new DeferredObject();
//        /* Extracting promise */
//        Promise promise = deferred.promise();
//
//        /* return promise with the async operation */
//        this.rpc.call("ex_getGraphList", msg).then((DoneCallback) (obj) -> {
//
//
//
//        });

    }

    /*================================ VERTEX EXTERNAL API METHODS ================================*/

    public Promise createVertex(Vertex v) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(v);

        /* return promise with the async operation */
        return this.rpc.call("ex_createVertex", msg);
    }


    public Promise updateVertex(Vertex v) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(v);

        /* return promise with the async operation */
        return this.rpc.call("ex_updateVertex", msg);
    }


    public Promise deleteVertex(Vertex v) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(v);

        /* return promise with the async operation */
        return this.rpc.call("ex_deleteVertex", msg);
    }

    public Promise getVertex(Vertex v) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(v);

        //TODO: Expect more results, fix this.

        /* return promise with the async operation */
        return this.rpc.call("ex_getVertex", msg);
    }

    public Promise getVertexList(Vertex v) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(v);

        //TODO: Expect more results, fix this.

        /* return promise with the async operation */
        return this.rpc.call("ex_getVertexList", msg);
    }

    /*================================ EDGE EXTERNAL API METHODS ================================*/

    public Promise createEdge(Edge e) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(e);

        /* return promise with the async operation */
        return this.rpc.call("ex_createEdge", msg);
    }

    public Promise updateEdge(Edge e) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(e);

        /* return promise with the async operation */
        return this.rpc.call("ex_updateEdge", msg);
    }

    public Promise deleteEdge(Edge e) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(e);

        /* return promise with the async operation */
        return this.rpc.call("ex_deleteEdge", msg);
    }

    public Promise getEdge(Edge e) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(e);

        //TODO: Expect more results, fix this.

        /* return promise with the async operation */
        return this.rpc.call("ex_getEdge", msg);
    }

    public Promise getEdgeList(Edge e) {

        /* validating connection */
        this.checkConnectionAndValidate();

        /* packing message */
        Message msg = new Message();
        msg.setPayload(e);

        //TODO: Expect more results, fix this.

        /* return promise with the async operation */
        return this.rpc.call("ex_getEdgeList", msg);
    }

    private void checkConnectionAndValidate() {

        if (!this.isConnected) {
            throw new Error("Client driver not connected to database.");
        }

    }
}
