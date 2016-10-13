package org.trueno.driver.lib.core.communication;

import com.github.nkzawa.socketio.client.Socket;

/**
 * <b>Callback Interface</b>
 * <p>Interface of Object received by remote operations in the RPC class.</p>
 *
 * @author Victor Santos
 * @version 0.1.0
 */
public interface Callback {

    void method(Socket socket);
}
