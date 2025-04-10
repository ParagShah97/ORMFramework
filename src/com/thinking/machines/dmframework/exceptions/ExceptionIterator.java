package com.thinking.machines.dmframework.exceptions;

import java.util.*;
import com.thinking.machines.dmframework.pojo.*;

public class ExceptionIterator {
    private String property;
    private int index;
    private String exception;
    private Iterator<Pair<Integer, String>> iterator;
    private LinkedList<Pair<Integer, String>> exceptions;

    ExceptionIterator(String property, LinkedList<Pair<Integer, String>> exceptions) {
        this.property = property;
        this.exceptions = exceptions;
        this.iterator = this.exceptions.iterator();
    }

    public String property() {
        return this.property;
    }

    public int index() {
        return this.index;
    }

    public String exception() {
        return this.exception;
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    public void next() {
        Pair<Integer, String> pair;
        pair = iterator.next();
        this.index = pair.getFirst();
        this.exception = pair.getSecond();
    }
}
