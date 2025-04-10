package com.thinking.machines.dmframework.exceptions;

public class DMFrameworkException extends Exception {
    private String databaseOperationException;

    public DMFrameworkException(String message) {
        super(message);
        databaseOperationException = "";
    }

    public DMFrameworkException(String message, String databaseOperationException) {
        super(message);
        this.databaseOperationException = databaseOperationException;
    }

    public String toString() {
        return super.getMessage();
    }

    public String getMessage() {
        return super.getMessage();
    }

    public void setDatabaseOperationException(String databaseOperationException) {
        this.databaseOperationException = databaseOperationException;
    }

    public String getDatabaseOperationException() {
        return this.databaseOperationException;
    }
}