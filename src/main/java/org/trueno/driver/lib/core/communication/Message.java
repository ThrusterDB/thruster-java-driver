package org.trueno.driver.lib.core.communication;

/**
 * Created by victor on 5/10/16.
 */


public class Message {

    private String message;
    private String time;

    public Message() {
    }

    public Message(String message, String time) {
        super();
        this.message = message;
        this.time = time;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}
