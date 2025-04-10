package com.thinking.machines.dmframework.exceptions;

import java.util.*;
import com.thinking.machines.dmframework.pojo.*;

public class ValidatorException extends Exception {
    private String databaseOperationException;
    private HashMap<String, LinkedList<Pair<Integer, String>>> exceptions = new HashMap<String, LinkedList<Pair<Integer, String>>>();

    public ValidatorException() {
        super("Validator Exception");
    }

    public HashMap<String, LinkedList<Pair<Integer, String>>> getExceptions() {
        return this.exceptions;
    }

    public boolean hasExceptions() {
        return this.exceptions.size() > 0;
    }

    public boolean contains(String key) {
        return this.exceptions.get(key) != null;
    }

    public void add(String key, String exception) {
        LinkedList<Pair<Integer, String>> linkedList;
        linkedList = this.exceptions.get(key);
        if (linkedList == null) {
            linkedList = new LinkedList<Pair<Integer, String>>();
            this.exceptions.put(key, linkedList);
        }
        linkedList.add(new Pair<Integer, String>(new Integer(linkedList.size() + 1), exception));
    }

    public void setDatabaseOperationException(String databaseOperationException) {
        this.databaseOperationException = databaseOperationException;
    }

    public String getDatabaseOperationException() {
        return this.databaseOperationException;
    }

    public ExceptionsIterator getIterator() {
        return new ExceptionsIterator(this);
    }
}
