package com.thinking.machines.dmframework.exceptions;

import java.util.*;
import com.thinking.machines.dmframework.pojo.*;

public class ExceptionsIterator {
    private ValidatorException validatorException;
    private HashMap<String, LinkedList<Pair<Integer, String>>> exceptions = new HashMap<String, LinkedList<Pair<Integer, String>>>();
    private Iterator<Map.Entry<String, LinkedList<Pair<Integer, String>>>> iterator;

    ExceptionsIterator(ValidatorException validatorException) {
        this.validatorException = validatorException;
        this.exceptions = validatorException.getExceptions();
        this.iterator = this.exceptions.entrySet().iterator();
    }

    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    public ExceptionIterator next() {
        Map.Entry<String, LinkedList<Pair<Integer, String>>> pair;
        pair = iterator.next();
        String key = pair.getKey();

        LinkedList<Pair<Integer, String>> value;
        value = pair.getValue();
        return new ExceptionIterator(key, value);
    }
}