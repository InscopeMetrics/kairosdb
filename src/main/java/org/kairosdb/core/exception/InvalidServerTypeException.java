package org.kairosdb.core.exception;

public class InvalidServerTypeException extends Exception {
    private static final long serialVersionUID = -7004099035951654258L;

    public InvalidServerTypeException(final String message) {
        super(message);
    }
}
