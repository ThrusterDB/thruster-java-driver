package experiments;

import org.trueno.driver.lib.core.Trueno;
import com.github.nkzawa.socketio.client.Socket;
import org.trueno.driver.lib.core.communication.Callback;
import org.trueno.driver.lib.core.data_structures.Graph;


/**
 * Created by victor on 7/20/16.
 */
public class GraphCreate {

    public static void main(String args[]){


        /* instantiate Trueno driver */
        final Trueno trueno  = new Trueno("http://localhost", 8000);

        System.out.println("Connecting...");

        /* Connecting */
        trueno.connect(new Callback() {
            public void method(Socket s) {

                System.out.println("connected " + s.id());
                System.out.println("------------------------Properties, computed, and meta-------------------------------");

                /* instantiate graph */
                Graph g = new Graph();


            }
        }, new Callback() {
            public void method(Socket socket) {
                System.out.println("disconnected");

            }
        });

    }
}
