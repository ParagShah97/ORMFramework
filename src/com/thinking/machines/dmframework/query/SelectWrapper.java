package com.thinking.machines.dmframework.query;

import com.thinking.machines.dmframework.pojo.*;
import java.util.*;
import java.lang.reflect.*;

public class SelectWrapper {
    private String sqlStatement;
    private HashMap<String, MethodWrapper> propertyMethodWrapperHashMap;
    private HashMap<String, Pair<Method, Method>> preparedStatementSetterAndResultSetGetterMethodsHashMap;
    private ArrayList<MethodWrapper> setterMethods;
    private ArrayList<Method> sqlGetterMethods;
    private ArrayList<Method> preparedStatementSetterMethods;
    private String defaultOrderBy;

    public SelectWrapper(String sqlStatement, ArrayList<MethodWrapper> setterMethods,
            ArrayList<Method> sqlGetterMethods, ArrayList<Method> preparedStatementSetterMethods,
            String defaultOrderBy) {
        this.sqlStatement = sqlStatement;
        this.setterMethods = setterMethods;
        this.sqlGetterMethods = sqlGetterMethods;
        this.preparedStatementSetterMethods = preparedStatementSetterMethods;
        this.propertyMethodWrapperHashMap = new HashMap<String, MethodWrapper>();
        this.preparedStatementSetterAndResultSetGetterMethodsHashMap = new HashMap<String, Pair<Method, Method>>();
        this.defaultOrderBy = defaultOrderBy;
        MethodWrapper methodWrapper;
        Method first, second;
        String property;
        int i = 0;
        while (i < setterMethods.size()) {
            methodWrapper = setterMethods.get(i);
            property = methodWrapper.getProperty();
            this.propertyMethodWrapperHashMap.put(property, methodWrapper);
            first = preparedStatementSetterMethods.get(i);
            second = sqlGetterMethods.get(i);
            this.preparedStatementSetterAndResultSetGetterMethodsHashMap.put(property,
                    new Pair<Method, Method>(first, second));
            i++;
        }
    }

    public MethodWrapper getMethodWrapperByProperty(String property) {
        MethodWrapper methodWrapper;
        methodWrapper = this.propertyMethodWrapperHashMap.get(property);
        return methodWrapper;
    }

    public Method getPreparedStatementSetterMethod(String property) {
        Pair<Method, Method> pair = preparedStatementSetterAndResultSetGetterMethodsHashMap.get(property);
        if (pair != null)
            return pair.getFirst();
        else
            return null;
    }

    public Method getResultSetGetterMethod(String property) {
        Pair<Method, Method> pair = preparedStatementSetterAndResultSetGetterMethodsHashMap.get(property);
        if (pair != null)
            return pair.getSecond();
        else
            return null;
    }

    public String getSQLStatement() {
        return this.sqlStatement;
    }

    public ArrayList<MethodWrapper> getSetterMethods() {
        return this.setterMethods;
    }

    public ArrayList<Method> getSQLGetterMethods() {
        return this.sqlGetterMethods;
    }

    public String getDefaultOrderBy() {
        return this.defaultOrderBy;
    }
}