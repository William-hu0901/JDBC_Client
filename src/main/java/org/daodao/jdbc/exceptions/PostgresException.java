package org.daodao.jdbc.exceptions;

public class PostgresException extends RuntimeException {
    public PostgresException(String message) {
        super(message);
    }

    public PostgresException(String message, Throwable cause) {
        super(message, cause);
    }
}