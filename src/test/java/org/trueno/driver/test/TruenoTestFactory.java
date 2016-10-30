package org.trueno.driver.test;

import org.trueno.driver.lib.core.TruenoFactory;
import org.trueno.driver.lib.core.data_structures.Graph;

/**
 * @author Edgardo Barsallo Yi
 */
public class TruenoTest {

    public static void main(String[] args) {

        Graph g = TruenoFactory.getInstance("graphi");
        g.open()
            .then((result) -> {
                System.out.println("open");
            })
            .fail((error)  -> {
                System.out.println("error");
            });

    }
}
