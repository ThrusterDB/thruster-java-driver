package experiments;

import org.trueno.driver.lib.core.Trueno;
import org.trueno.driver.lib.core.data_structures.Filter;
import org.trueno.driver.lib.core.data_structures.Graph;


/**
 * Created by: victor, miguel
 * Date: 7/20/16
 * Purpose:
 */

public class GraphCount {
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

            Filter filter = g.filter()
                    .term("prop.name", "aura");

            g.count("v", filter).whenCompleteAsync((ret, err) -> {
                if (ret != null) {
                    System.out.println("Info from Graph g counted " + ret.toString());
                } else {
                    throw new Error("Error: Could not count Graph g info " + err);
                }
            });

            g.count("v").whenCompleteAsync((ret, err) -> {
                if (ret != null) {
                    System.out.println("Total vertices in graph " + ret.toString());
                } else {
                    throw new Error("Could not count vertices " + err);
                }

            });
        }, socket -> System.out.println("disconnected"));

    }
}
