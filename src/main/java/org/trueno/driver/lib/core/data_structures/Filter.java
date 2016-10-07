package org.trueno.driver.lib.core.data_structures;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

/**
 * Created by: victor, miguel
 * Date: 9/6/16
 * Purpose:
 */

public class Filter {

    private ArrayList<JSONObject> filters;
    private String ftr;

    /**
     * Create a Filter object instance.
     */
    public Filter() {

        this.filters = new ArrayList<>();
        this.ftr = "";
    }

    /**
     * Returns an array of all filters.
     */
    public ArrayList<JSONObject> getFilters() {
        return this.filters;
    }

    /**
     * Clear stored filters.
     */
    public void clearFilters() {
        this.filters.clear();
    }

    /**
     * The term matching filter, can be either a exact string or number.
     *
     * @param prop The property/meta/computed to be applied on the operation.
     * @param val  The filter value.
     */
    public Filter term(String prop, Object val) {

        JSONObject json = new JSONObject();

        try {

            json.put("type", "term");
            json.put("prop", prop);
            json.put("val", val);
            json.put("ftr", this.ftr.isEmpty() ? "AND" : this.ftr);
            filters.add(json);
        } catch (Exception e) {
            throw new RuntimeException("Error while manipulating JSON Object", e);
        }

        /* reset filter */
        this.ftr = "";

        return this;
    }

    /**
     * The range filter.
     *
     * @param prop The property/meta/computed to be applied on the operation.
     * @param op   The inequality operator(gt,gte,lt,lte).
     * @param val  The filter date or number value.
     */
    public Filter range(String prop, String op, Object val) {

        JSONObject json = new JSONObject();

        try {

            json.put("type", "range");
            json.put("prop", prop);
            json.put("op", op);
            json.put("val", val);
            json.put("ftr", this.ftr.isEmpty() ? "AND" : this.ftr);
            filters.add(json);
        } catch (Exception e) {
            throw new RuntimeException("Error while manipulating JSON Object", e);
        }

        /* reset filter */
        this.ftr = "";

        return this;
    }

    /**
     * The exist check field filter.
     *
     * @param prop The property/meta/computed to be applied on the operation.
     */
    public Filter exist(String prop) {

        JSONObject json = new JSONObject();

        try {
            json.put("type", "exist");
            json.put("prop", prop);
            json.put("ftr", this.ftr.isEmpty() ? "AND" : this.ftr);
            filters.add(json);
        } catch (Exception e) {
            throw new RuntimeException("Error while manipulating JSON Object", e);
        }
        /* reset filter */
        this.ftr = "";

        return this;
    }

    /**
     * The wildcard string search filter.
     *
     * @param prop The property/meta/computed to be applied on the operation.
     * @param val  The wildcard filter value.
     */
    public Filter wildcard(String prop, String val) {

        JSONObject json = new JSONObject();

        try {
            json.put("type", "wildcard");
            json.put("val", val);
            json.put("prop", prop);
            json.put("ftr", this.ftr.isEmpty() ? "AND" : this.ftr);
            filters.add(json);
        } catch (Exception e) {
            throw new RuntimeException("Error while manipulating JSON Object", e);
        }

        /* reset filter */
        this.ftr = "";

        return this;
    }

    /**
     * The regexp string search filter.
     *
     * @param prop The property/meta/computed to be applied on the operation.
     * @param val  The filter regular expression value.
     */
    public Filter regexp(String prop, String val) {

        JSONObject json = new JSONObject();

        try {
            json.put("type", "regexp");
            json.put("prop", prop);
            json.put("val", val);
            json.put("ftr", this.ftr.isEmpty() ? "AND" : this.ftr);
            filters.add(json);
        } catch (Exception e) {
            throw new RuntimeException("Error while manipulating JSON Object", e);
        }

        /* reset filter */
        this.ftr = "";

        return this;
    }

    /**
     * The prefix string search filter.
     *
     * @param prop The property/meta/computed to be applied on the operation.
     * @param val  The filter value.
     */
    public Filter prefix(String prop, Object val) {

        JSONObject json = new JSONObject();

        try {
            json.put("type", "prefix");
            json.put("prop", prop);
            json.put("val", val);
            json.put("ftr", this.ftr.isEmpty() ? "AND" : this.ftr);
            filters.add(json);
        } catch (Exception e) {
            throw new RuntimeException("Error while manipulating JSON Object", e);
        }

        /* reset filter */
        this.ftr = "";

        return this;
    }

    /**
     * Limit the results of this search.
     *
     * @param val The integer limit of for the results.
     */
    public Filter limit(int val) {

        JSONObject json = new JSONObject();
        /* Trying to put json values */
        try {
            json.put("type", "size");
            json.put("val", val);
        } catch (Exception e) {
            throw new RuntimeException("Error while manipulating JSON Object", e);
        }
        /* set object */
        filters.add(json);

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
