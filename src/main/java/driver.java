
import org.jdeferred.DoneCallback;
import org.trueno.driver.lib.core.Trueno;
import com.github.nkzawa.socketio.client.Socket;
import org.trueno.driver.lib.core.communication.Callback;
import org.trueno.driver.lib.core.data_structures.Graph;


/**
 * Created by victor on 7/20/16.
 */
public class driver {

    public static void main(String args[]){
        System.out.println("Begins here...");

        /* instantiate Trueno driver */
        final Trueno trueno  = new Trueno("http://localhost", 8000);

        /* Connecting */
        trueno.connect(new Callback() {
            public void method(Socket socket) {
                System.out.println("Connected " + socket.id());

                Graph g = new Graph();
                g.setAttribute("field", "Computer Science");
                g.setAttribute("classes", 33);
                g.setAttribute("Size", 20123);

                trueno.createGraph(g).then(new DoneCallback() {
                    public void onDone(Object o) {
                        System.out.println("Reponse from creating graph: " + o);
                    }
                });

                trueno.getGraphList(null).then(new DoneCallback() {
                    public void onDone(Object o) {
                        System.out.println("Reponse from creating graph: " + o);
                    }
                });
            }
        }, new Callback() {
            public void method(Socket socket) {
                System.out.println("Disconnected");

            }
        });

    }
}
