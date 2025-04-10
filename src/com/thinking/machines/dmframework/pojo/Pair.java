package com.thinking.machines.dmframework.pojo;

public class Pair<T1, T2> {
    private Object first;
    private Object second;

    public Pair(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }

    public T1 getFirst() {
        return (T1) this.first;
    }

    public T2 getSecond() {
        return (T2) this.second;
    }

    public void setFirst(T1 first) {
        this.first = first;
    }

    public void setSecond(T2 second) {
        this.second = second;
    }
}