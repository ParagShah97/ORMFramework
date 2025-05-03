package com.thinking.machines.dmframework.dml;

import java.lang.reflect.*;
import com.thinking.machines.dmframework.pojo.*;

public class SequenceWrapper {
    private String nextValSQLStatement;
    private MethodWrapper nextValObjectPropertyGetter;
    private MethodWrapper nextValObjectPropertySetter;
    private Method nextValResultSetGetter;

    public SequenceWrapper(String nextValSQLStatement, MethodWrapper nextValObjectPropertySetter,
            MethodWrapper nextValObjectPropertyGetter, Method nextValResultSetGetter) {
        this.nextValSQLStatement = nextValSQLStatement;
        this.nextValObjectPropertySetter = nextValObjectPropertySetter;
        this.nextValObjectPropertyGetter = nextValObjectPropertyGetter;
        this.nextValResultSetGetter = nextValResultSetGetter;
    }

    public String getNextValSQLStatement() {
        return this.nextValSQLStatement;
    }

    public MethodWrapper getNextValObjectPropertySetter() {
        return this.nextValObjectPropertySetter;
    }

    public MethodWrapper getNextValObjectPropertyGetter() {
        return this.nextValObjectPropertyGetter;
    }

    public Method getNextValResultSetGetter() {
        return this.nextValResultSetGetter;
    }
}