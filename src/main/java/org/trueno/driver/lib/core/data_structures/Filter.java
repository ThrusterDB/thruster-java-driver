package org.trueno.driver.lib.core.data_structures;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <b>Filter Class</b>
 * <p>Filter that can be applied to a Graph search</p>
 *
 * @author Victor Santos
 * @author Miguel Rivera
 * @version 0.1.0
 */
public class Filter {

    private JSONArray filters;
    private String ftr;

    private final Logger log = LoggerFactory.getLogger(Filter.class.getName());

    /**
     * Create a Filter object instance.
     */
    public Filter() {

        this.filters = new JSONArray();
        this.ftr = "";

        log.trace("Filter Object created");
    }

    /**
     * Returns an array of all filters.
     */
    public JSONArray getFilters() {
        return this.filters;
    }

    /**
     * Clear stored filters.
     */
    public void clearFilters() {
        this.filters = new JSONArray();
    }

    /**
     * The term matching filter, can be either a exact string or number.
     *
     * @param prop
     *         The property/meta/computed to be applied on the operation.
     * @param val
     *         The filter value.
     */
    public Filter term(String prop, Object val) {
        JSONObject json = new JSONObject();

        json.put("type", "term");
        json.put("prop", prop);
        json.put("val", val);
        json.put("ftr", this.ftr.isEmpty() ? "AND" : this.ftr);
        filters.put(json);

        /* reset filter */
        this.ftr = "";

        return this;
    }

    /**
     * The range filter.
     *
     * @param prop
     *         The property/meta/computed to be applied on the operation.
     * @param op
     *         The inequality operator(gt,gte,lt,lte).
     * @param val
     *         The filter date or number value.
     */
    public Filter range(String prop, String op, Object val) {

        JSONObject json = new JSONObject();

        json.put("type", "range");
        json.put("prop", prop);
        json.put("op", op);
        json.put("val", val);
        json.put("ftr", this.ftr.isEmpty() ? "AND" : this.ftr);
        filters.put(json);

        /* reset filter */
        this.ftr = "";

        return this;
    }

    /**
     * The exist check field filter.
     *
     * @param prop
     *         The property/meta/computed to be applied on the operation.
     */
    public Filter exist(String prop) {

        JSONObject json = new JSONObject();

        json.put("type", "exist");
        json.put("prop", prop);
        json.put("ftr", this.ftr.isEmpty() ? "AND" : this.ftr);
        filters.put(json);

        /* reset filter */
        this.ftr = "";

        return this;
    }

    /**
     * The wildcard string search filter.
     *
     * @param prop
     *         The property/meta/computed to be applied on the operation.
     * @param val
     *         The wildcard filter value.
     */
    public Filter wildcard(String prop, String val) {

        JSONObject json = new JSONObject();

        json.put("type", "wildcard");
        json.put("val", val);
        json.put("prop", prop);
        json.put("ftr", this.ftr.isEmpty() ? "AND" : this.ftr);
        filters.put(json);

        /* reset filter */
        this.ftr = "";

        return this;
    }

    /**
     * The regexp string search filter.
     *
     * @param prop
     *         The property/meta/computed to be applied on the operation.
     * @param val
     *         The filter regular expression value.
     */
    public Filter regexp(String prop, String val) {

        JSONObject json = new JSONObject();

        json.put("type", "regexp");
        json.put("prop", prop);
        json.put("val", val);
        json.put("ftr", this.ftr.isEmpty() ? "AND" : this.ftr);
        filters.put(json);

        /* reset filter */
        this.ftr = "";

        return this;
    }

    /**
     * The prefix string search filter.
     *
     * @param prop
     *         The property/meta/computed to be applied on the operation.
     * @param val
     *         The filter value.
     */
    public Filter prefix(String prop, Object val) {

        JSONObject json = new JSONObject();

        json.put("type", "prefix");
        json.put("prop", prop);
        json.put("val", val);
        json.put("ftr", this.ftr.isEmpty() ? "AND" : this.ftr);
        filters.put(json);

        /* reset filter */
        this.ftr = "";

        return this;
    }

    /**
     * Limit the results of this search.
     *
     * @param val
     *         The integer limit of for the results.
     */
    public Filter limit(int val) {

        JSONObject json = new JSONObject();

        json.put("type", "size");
        json.put("val", val);

        /* set object */
        filters.put(json);

        return this;
    }

    /**
     * Chooses next filter with an OR operator.
     */
    public Filter or() {
    /* set OR filter to true */
        this.ftr = "OR";
        return this;
    }

    /**
     * Chooses next filter with an NOT operator.
     */
    public Filter not() {
    /* set NOT filter*/
        this.ftr = "NOT";
        return this;
    }
}
