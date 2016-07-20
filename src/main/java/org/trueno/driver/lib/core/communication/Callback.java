package org.trueno.driver.lib.core.communication;
import com.github.nkzawa.socketio.client.Socket;
/**
 * Created by victor on 7/20/16.
 */
public interface Callback {

    void method(Socket socket);
}
