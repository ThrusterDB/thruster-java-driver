
import org.jdeferred.DoneCallback;
import org.trueno.driver.lib.core.Trueno;
import com.github.nkzawa.socketio.client.Socket;
import org.trueno.driver.lib.core.communication.Callback;
import org.trueno.driver.lib.core.communication.Message;
import org.trueno.driver.lib.core.data_structures.Graph;
import org.trueno.driver.lib.core.data_structures.Vertex;


/**
 * Created by victor on 7/20/16.
 */
public class driver {

    public static void main(String args[]){
        System.out.println("Begins here...");

        /* instantiate Trueno driver */
        final Trueno trueno  = new Trueno("http://localhost", 8000);

        System.out.println("Connecting...");

        /* Connecting */
        trueno.connect(new Callback() {
            public void method(Socket socket) {
                System.out.println("Connected " + socket.id());

                Graph g = new Graph();
                g.setGraphid("java_test");
                g.setId("java_test");

                g.setAttribute("field", "Computer Science");
                g.setAttribute("classes", 33);
                g.setAttribute("Size", 20123);

                trueno.createGraph(g).then((DoneCallback) (o1) -> {

                    System.out.println("Reponse from creating graph: " + o1);

                    Vertex v = new Vertex();
                    v.setGraphid("java_test");
                    v.setId("1");
                    v.setAttribute("label", 10);
                    v.setPartition(1);

                    trueno.createVertex(v).then((DoneCallback) (o2) -> {
                        System.out.println("Response from creating vertex: " + o2);
                    });

                    trueno.getGraphList(null).then(new DoneCallback() {
                        public void onDone(Object o) {

                            System.out.println("Reponse from listing graph: " + o);
                        }
                    });

                });

            }
        }, new Callback() {
            public void method(Socket socket) {
                System.out.println("Disconnected");

            }
        });

        System.out.println("end!");

    }
}
