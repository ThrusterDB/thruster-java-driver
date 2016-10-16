package org.trueno.driver.lib.core.data_structures;

import org.jdeferred.Deferred;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trueno.driver.lib.core.communication.Message;

/**
 * Compute Data Structure.
 *
 * @author Miguel Rivera
 * @version 0.1.0
 */
public class Compute extends Component {

    private JSONObject computeParameters;
    private Algorithm algorithm;

    private final Logger log = LoggerFactory.getLogger(Compute.class.getName());

    /**
     * Create Compute Instance. Initialize fields, set Component type to 'c'.
     */
    public Compute() {
        this.setType("c");

        setAlgorithm(Algorithm.NONE);
        setComputeParameters(new JSONObject());
    }

    /**
     * Get the current parameters to be used for the compute operations
     * @return parameters in JSON format
     */
    public JSONObject getComputeParameters() {
        return computeParameters;
    }

    /**
     * Set parameters for compute operations
     * @param computeParameters Parameters to be used in the compute operations.
     */
    public void setComputeParameters(JSONObject computeParameters) {
        this.computeParameters = computeParameters;
    }

    /**
     * Get algorithm to be used in the compute operation
     * @return algorithm name
     */
    public String getAlgorithm() {
        return algorithm.toString();
    }

    /**
     * Set the algorithm to be used in the compute operations
     * @param algorithm new algorithm name
     */
    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    /**
     * Deploy the algorithm in the Spark Cluster - using Spark Job Server
     *
     * @return Promise with Job ID in JSON format
     */
    public Promise<JSONObject, JSONObject, Integer> deploy() {
        final String apiFun = "ex_compute";

        /* Instantiating deferred object */
        final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
        /* Extracting promise */
        Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

        if (!this.validateGraphLabel()) {
            deferred.reject(new JSONObject().put("error", "Graph label not set"));
            return promise;
        }

        this.setId(this.getLabel());

        Message msg = new Message();

        msg.setPayload(new JSONObject()
                .put("graph", this.getLabel())
                .put("algorithmType", this.algorithm)
                .put("subgraph", "schema")
                .put("parameters", this.computeParameters));

        log.trace("{} – {}", apiFun, msg.toString(2));

        this.getParentGraph().getConn().call(apiFun, msg).then(message -> {
            this.setJobId(((Component)message).getJobId());
            deferred.resolve(message);
        }, deferred::reject);

        return promise;
    }

    /**
     * Returns a job status.
     * @param jobId the job whose status will be returned.
     * @return the status of the job.
     */
    public Promise<JSONObject, JSONObject, Integer> jobStatus(String jobId) {
        final String apiFun = "ex_computeJobStatus";

        if (!this.validateGraphLabel()) {
            final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
            Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

            deferred.reject(new JSONObject().put("error", "Graph label not set"));
            return promise;
        }

        Message msg = new Message();

        msg.setPayload(new JSONObject().put("jobId", jobId));

        log.trace("{} – {}", apiFun, msg.toString(2));

        return this.getParentGraph().getConn().call(apiFun, msg);
    }

    /**
     * Returns the result of a job.
     * @param jobId the job whose result will be returned.
     * @return the result of the job.
     */
    public Promise<JSONObject, JSONObject, Integer> jobResult(String jobId) {
        final String apiFun = "ex_computeJobResult";

        if (!this.validateGraphLabel()) {
            final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
            Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

            deferred.reject(new JSONObject().put("error", "Graph label not set"));
            return promise;
        }

        Message msg = new Message();

        msg.setPayload(new JSONObject().put("jobId", jobId));

        log.trace("{} – {}", apiFun, msg.toString(2));

        return this.getParentGraph().getConn().call(apiFun, msg);
    }
}

