package org.trueno.driver.lib.core.communication;

import com.corundumstudio.socketio.SocketIOClient;
import java.util.HashMap;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by victor on 7/19/16.
 */
public class RPC {

    /* Private properties */
    private String host;
    private int port;
    private HashMap<String, Method> procedures;
    private SocketIOClient socket;

    /* Default Constructor */
    public RPC(){

        /* Set default properties */
        this.host = "http://localhost";
        this.port = 8000;
        this.procedures = new HashMap<String, Method>();
        this.socket = null;
    }
    /* Constructor with Parameters */
    public RPC(String host, Integer port){

        /* calling default constructor */
        this();
        /* Set parameters */
        this.host  = host != null ? host : this.host;
        this.port = port != null ? port : this.port;
    }

    /* Public methods */
    public void expose(String procedureName, Method procedureFunction) {
    /* Insert the procedure in the collection */
        this.procedures.put(procedureName, procedureFunction);
    }

    public void call() {


    }

    public void connect() {


    }



}
