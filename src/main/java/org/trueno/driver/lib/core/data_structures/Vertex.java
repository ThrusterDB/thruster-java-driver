package org.trueno.driver.lib.core.data_structures;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by victor on 7/19/16.
 */
public class Vertex extends Component {
    //TODO: Implement the same as Component.java, using Vertex.js as guide.


    // FIXME: Probably the use of the partition field will have to be redefined.
    public String getPartition() {
        try {
            return (String) ((JSONObject) this.get("_property")).get("_partition");
        } catch (JSONException e) {
            System.out.println(e);
        }
        return null;
    }

    public void setPartition(int partition) {
        try {
            ((JSONObject) this.get("_property")).put("_partition",partition);
        } catch (JSONException e) {
            System.out.println(e);
        }
    }

}
