package experiments;

import org.trueno.driver.lib.core.Trueno;
import org.trueno.driver.lib.core.data_structures.Graph;

/**
 * Created by: victor, miguel
 * Date: 7/20/16
 * Purpose:
 */

public class GraphCreate {

    public static void main(String args[]) {
        /* instantiate Trueno driver */
        final Trueno trueno = new Trueno("http://localhost", 8000);

        System.out.println("Connecting...");

        /* Connecting */
        trueno.connect(s -> {

            System.out.println("connected " + s.id());
            System.out.println("------------------------Properties, computed, and meta-------------------------------");

            /* instantiate graph */
            Graph g = trueno.Graph("graphi");

            /* Adding properties and computed fields */
            g.setProperty("version", 1);

            g.setComputed("pagerank", "average", 2.55);
            g.setComputed("pagerank", "low", 1);


            g.create().whenCompleteAsync((ret, err) -> {
                if (ret != null) {
                    System.out.println("Graph g created " + ret.toString());
                } else {
                    throw new Error("Error: Graph g creation failed " + err);
                }
            });

        }, socket -> System.out.println("disconnected"));

    }
}
