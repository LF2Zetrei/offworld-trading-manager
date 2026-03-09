package com.offworld.api;

/**
 * Represents an HTTP error response from the game server.
 */
public class ApiException extends RuntimeException {
    private final int statusCode;
    private final String body;

    public ApiException(int statusCode, String body) {
        super("HTTP " + statusCode + ": " + body);
        this.statusCode = statusCode;
        this.body = body;
    }

    public int getStatusCode() { return statusCode; }
    public String getBody() { return body; }

    public boolean isUnauthorized()  { return statusCode == 401; }
    public boolean isForbidden()     { return statusCode == 403; }
    public boolean isNotFound()      { return statusCode == 404; }
    public boolean isConflict()      { return statusCode == 409; }
    public boolean isUnavailable()   { return statusCode == 503; }
    public boolean isBadRequest()    { return statusCode == 400; }
}
