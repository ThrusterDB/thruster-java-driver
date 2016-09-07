package experiments;

import org.jdeferred.DoneCallback;
import org.jdeferred.FailCallback;
import org.json.JSONObject;
import org.trueno.driver.lib.core.Trueno;
import com.github.nkzawa.socketio.client.Socket;
import org.trueno.driver.lib.core.communication.Callback;
import org.trueno.driver.lib.core.communication.ErrorCallback;
import org.trueno.driver.lib.core.communication.ResultCallback;
import org.trueno.driver.lib.core.data_structures.Graph;


/**
 * Created by victor on 7/20/16.
 */
public class GraphCreate {

    public static void main(String args[]) {


        /* instantiate Trueno driver */
        final Trueno trueno = new Trueno("http://localhost", 8000);

        System.out.println("Connecting...");

        /* Connecting */
        trueno.connect(new Callback() {
            public void method(Socket s) {

                System.out.println("connected " + s.id());
                System.out.println("------------------------Properties, computed, and meta-------------------------------");

                /* instantiate graph */
                Graph g = trueno.Graph("graphi");

                /* Adding properties and computed fields */
                g.setProperty("version", 1);

                g.setComputed("pagerank", "average", 2.55);
                g.setComputed("pagerank", "low", 1);


                g.create().then(new ResultCallback() {
                    public void onDone(JSONObject result) {
                        System.out.println("Graph g created " + result);
                    }
                }, new ErrorCallback() {
                    public void onFail(JSONObject error) {
                        System.out.println("Error: Graph g creation failed " + error);
                    }
                });


            }
        }, new Callback() {
            public void method(Socket socket) {
                System.out.println("disconnected");

            }
        });

    }
}
