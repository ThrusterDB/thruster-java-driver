package org.trueno.driver.lib.core.communication;
import com.github.nkzawa.socketio.client.Socket;

/**
 * Created by: victor
 * Date: 7/20/16
 * Purpose:
 */

public interface Callback {

    void method(Socket socket);
}
