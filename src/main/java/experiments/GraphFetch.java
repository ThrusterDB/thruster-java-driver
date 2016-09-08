package experiments;

import com.github.nkzawa.socketio.client.Socket;
import org.json.JSONObject;
import org.trueno.driver.lib.core.Trueno;
import org.trueno.driver.lib.core.communication.Callback;
import org.trueno.driver.lib.core.communication.ErrorCallback;
import org.trueno.driver.lib.core.communication.ResultCallback;
import org.trueno.driver.lib.core.data_structures.Filter;
import org.trueno.driver.lib.core.data_structures.Graph;


/**
 * Created by victor on 7/20/16.
 */
public class GraphFetch {

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

                Filter filter = g.filter()
                        .term("prop.version", 1);

                Filter filter2 = g.filter()
                        .term("prop.name", "aura");


                g.fetch("g", filter).then(new ResultCallback() {
                    public void onDone(JSONObject result) {
                        System.out.println("Info from Graph g fetched " + result);
                    }
                }, new ErrorCallback() {
                    public void onFail(JSONObject error) {
                        System.out.println("Error: Could not fetch Graph g info " + error);
                    }
                });

                g.fetch("v", filter2).then(new ResultCallback() {
                    public void onDone(JSONObject result) {
                        System.out.println("Info from Graph g component v fetched " + result);
                    }
                }, new ErrorCallback() {
                    public void onFail(JSONObject error) {
                        System.out.println("Error: Could not fetch Graph g component v info " + error);
                    }
                });

                g.fetch("e").then(new ResultCallback() {
                    public void onDone(JSONObject result) {
                        System.out.println("All edges " + result);
                    }
                }, new ErrorCallback() {
                    public void onFail(JSONObject error) {
                        System.out.println("Error: fetching all edges " + error);
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
