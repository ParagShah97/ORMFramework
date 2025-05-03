package com.thinking.machines.dmframework.dml;

import com.thinking.machines.dmframework.pojo.*;
import java.util.*;
import java.lang.reflect.*;
import com.thinking.machines.dmframework.dml.*;

public class InsertWrapper {
    private String sqlStatement;
    private ArrayList<MethodWrapper> getterMethods;
    private ArrayList<Method> preparedStatementSetterMethods;
    private MethodWrapper autoIncrementedPropertySetter;
    private Method autoIncrementedPropertyResultSetGetter;
    private HashMap<Integer, SequenceWrapper> sequenceWrappers;

    public InsertWrapper(String sqlStatement, ArrayList<MethodWrapper> getterMethods,
            ArrayList<Method> preparedStatementSetterMethods, MethodWrapper autoIncrementedPropertySetter,
            Method autoIncrementedPropertyResultSetGetter, HashMap<Integer, SequenceWrapper> sequenceWrappers) {
        this.sqlStatement = sqlStatement;
        this.getterMethods = getterMethods;
        this.preparedStatementSetterMethods = preparedStatementSetterMethods;
        this.autoIncrementedPropertySetter = autoIncrementedPropertySetter;
        this.autoIncrementedPropertyResultSetGetter = autoIncrementedPropertyResultSetGetter;
        this.sequenceWrappers = sequenceWrappers;
    }

    public String getSQLStatement() {
        return this.sqlStatement;
    }

    public ArrayList<MethodWrapper> getGetterMethods() {
        return this.getterMethods;
    }

    public ArrayList<Method> getPreparedStatementSetterMethods() {
        return this.preparedStatementSetterMethods;
    }

    public MethodWrapper getAutoIncrementedPropertySetter() {
        return this.autoIncrementedPropertySetter;
    }

    public Method getAutoIncrementedPropertyResultSetGetter() {
        return this.autoIncrementedPropertyResultSetGetter;
    }

    public HashMap<Integer, SequenceWrapper> getSequenceWrappers() {
        return this.sequenceWrappers;
    }
}