package org.trueno.driver.lib.core.data_structures;

import org.jdeferred.Deferred;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.json.JSONObject;
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

    /**
     * Create Compute Instance. Initialize fields, set Component type to 'c'.
     */
    public Compute() {
        this.setType(ComponentType.COMPUTE);

        this.setAlgorithm(Algorithm.NONE);
        this.setComputeParameters(new JSONObject());
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
        final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
        Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

        if (!this.validateGraphLabel()) {
            log.error("Graph label is empty");
            deferred.reject(new JSONObject().put("error", "Graph label is empty"));
        }
        else {
            this.setId(this.getLabel());

            Message msg = new Message();

            msg.setPayload(new JSONObject()
                    .put("graph", this.getLabel())
                    .put("algorithmType", this.algorithm)
                    .put("subgraph", "schema")
                    .put("parameters", this.computeParameters));

            log.debug("{} – {}", apiFun, msg.toString(2));

            this.getParentGraph().getConn().call(apiFun, msg).then(message -> {
                this.setJobId(message.get("_jobId"));
                deferred.resolve(message);
            }, deferred::reject);
        }
        return promise;
    }

    /**
     * Returns a job status.
     * @param jobId the job whose status will be returned.
     * @return the status of the job.
     */
    public Promise<JSONObject, JSONObject, Integer> jobStatus(String jobId) {
        final String apiFun = "ex_computeJobStatus";
        final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
        Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

        if (!this.validateGraphLabel()) {
            log.error("Graph label is empty");
            deferred.reject(new JSONObject().put("error", "Graph label is empty"));
        }
        else {
            Message msg = new Message();

            msg.setPayload(new JSONObject().put("jobId", jobId));

            log.debug("{} – {}", apiFun, msg.toString(2));

            this.getParentGraph().getConn().call(apiFun, msg).then(message -> {
                this.setJobId(message.get("status"));
                deferred.resolve(message);
            }, deferred::reject);
        }

        return promise;
    }

    /**
     * Returns the result of a job.
     * @param jobId the job whose result will be returned.
     * @return the result of the job.
     */
    public Promise<JSONObject, JSONObject, Integer> jobResult(String jobId) {
        final String apiFun = "ex_computeJobResult";
        final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
        Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

        if (!this.validateGraphLabel()) {
            log.error("Graph label is empty");
            deferred.reject(new JSONObject().put("error", "Graph label is empty"));
            return promise;
        }
        else {
            Message msg = new Message();

            msg.setPayload(new JSONObject().put("jobId", jobId));

            log.debug("{} – {}", apiFun, msg.toString(2));

            this.getParentGraph().getConn().call(apiFun, msg).then(message -> {
                this.setJobId(message.get("result"));
                deferred.resolve(message);
            }, deferred::reject);
        }

        return promise;
    }

    /**
     *
     */
    public Promise<JSONObject, JSONObject, Integer> getAlgorithms() {
        final String apiFun = "ex_getComputeAlgorithms";
        final Deferred<JSONObject, JSONObject, Integer> deferred = new DeferredObject<>();
        Promise<JSONObject, JSONObject, Integer> promise = deferred.promise();

        Message msg = new Message();

        msg.setPayload(new JSONObject().put("getAlgorithms", true));

        log.debug("{} – {}", apiFun, msg.toString(2));

        this.getParentGraph().getConn().call(apiFun, msg).then(deferred::resolve, deferred::reject);

        return promise;
    }
}

