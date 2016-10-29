package org.trueno.driver.lib.core;

import net.jcip.annotations.NotThreadSafe;
import org.junit.runner.notification.RunListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.trueno.driver.lib.core.communication.RPC;
import org.trueno.driver.lib.core.data_structures.Graph;

import java.util.HashMap;
import java.util.Map;

/**
 * TruenoDB Java Driver – Trueno Database connection factory
 *
 * @author Miguel Rivera
 * @author Edgardo Barsallo Yi
 * @version 0.1.0
 */
@NotThreadSafe
public class TruenoFactory {

    /* Configuration constants */
    public static final String CONFIG_DATABASE = "trueno.storage.database";
    public static final String CONFIG_HOST = "trueno.storage.server";
    public static final String CONFIG_PORT = "trueno.storage.port";

    private static final Logger log = LoggerFactory.getLogger(Trueno.class.getName());

    /* Trueno API */
    private static Trueno trueno;


    protected TruenoFactory() {

    }

    public static Trueno getTruenoFactory(Object host, Object port) {
        if (host == null || port == null) {
            trueno = new Trueno();
        } else {
            trueno = new Trueno(host.toString(), Integer.parseInt(port.toString()));
        }

        trueno.connect(
            /* on connect */
                socket -> {
                    log.info("[{}] connected", socket.id());
                },
            /* on disconnect */
                socket -> {
                    log.info("[{}] disconnected", socket.id());
                }
        );

        return trueno;
    }

    /* FIXME: Credentials are ignored once a connection are created. */
    public static Graph getInstance(final Map<String,Object> config) {
        String database = config.get(CONFIG_DATABASE).toString();
        Object host = config.get(CONFIG_HOST);
        Object port = config.get(CONFIG_PORT);

        if (trueno == null)
            return getTruenoFactory(host, port).Graph(database);
        else
            return trueno.Graph(database);
    }

    public static Graph getInstance(String database) {
        HashMap<String, Object> map = new HashMap();
        map.put(CONFIG_DATABASE, database);

        if (trueno == null)
            return getInstance(map);
        else
            return trueno.Graph(database);
    }
}
