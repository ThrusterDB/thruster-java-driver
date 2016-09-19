package experiments;

import org.trueno.driver.lib.core.Trueno;
import org.trueno.driver.lib.core.data_structures.Filter;
import org.trueno.driver.lib.core.data_structures.Graph;

/**
 * Created by: victor, miguel
 * Date: 7/20/16
 * Purpose:
 */

public class GraphFetch {

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
                    .term("prop.version", 1);

            Filter filter2 = g.filter()
                    .term("prop.name", "aura");


            g.fetch("g", filter).whenComplete((result, error) -> {
                if (result != null) {
                    System.out.println("Info from Graph g component v fetched " + result.toString());
                }
                else {
                    throw new Error("Could not fetch information from graph", error);
                }
            });

            g.fetch("v", filter2).whenCompleteAsync((ret, err) -> {
                if (ret != null) {
                    System.out.println("Info from Graph g component v fetched " + ret.toString());
                }
                else {
                    throw new Error("Error: Could not fetch Graph g component v info " + err);
                }
            });

            g.fetch("e").whenCompleteAsync((ret, err) -> {
                if (ret != null) {
                    System.out.println("All edges " + ret.toString());
                }
                else {
                    throw new Error("Error: fetching all edges " + err);
                }
            });
        }, socket -> System.out.println("disconnected"));

    }
}
