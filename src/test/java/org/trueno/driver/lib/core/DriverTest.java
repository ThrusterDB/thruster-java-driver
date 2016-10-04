package org.trueno.driver.lib.core;

import org.json.JSONObject;
import org.trueno.driver.lib.core.data_structures.Filter;
import org.trueno.driver.lib.core.data_structures.Graph;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author Edgardo Barsallo Yi (ebarsallo)
 */
public class DriverTest {

    public static void main(String[] args) {

        System.out.println("Testing java-driver...");
        Trueno trueno = new Trueno("http://localhost", 8000);
        Graph g = trueno.Graph("graphi");

        trueno.connect(socket -> {
            /* connect */
            System.out.println("connected");
            Filter filter = g.filter().term("prop.age", 65);

            try {
                JSONObject test = g.fetch("v", filter).get();
                System.out.println("fetch() 1.1 -> " + test);

                JSONObject obj = new JSONObject();
                CompletableFuture<JSONObject> future = g.fetch("v", filter);

                System.out.println("fetch() 2.1 -> " + future.get());
                System.out.println("fetch() 2.2 -> " + future.get());
                g.fetch("v", filter).thenApplyAsync((result) -> {
                    System.out.println("fetch() 2.3 -> " + result);
                    return result;
                });
                System.out.println("fetch() 2.4 -> " + g.fetch("v").get(10, TimeUnit.SECONDS));

            } catch (Exception ex) {
                System.out.println("fetch() 1 error: " + ex);
            }

        }, socket -> {
            /* disconnect */
            System.out.println("disconnected");
        });

    }

}
