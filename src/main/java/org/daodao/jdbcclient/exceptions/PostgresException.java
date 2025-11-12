package org.daodao.jdbcclient.exceptions;

public class PostgresException extends Exception {
    public PostgresException(String message) {
        super(message);
    }

    public PostgresException(String message, Throwable cause) {
        super(message, cause);
    }
}