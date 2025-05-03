package com.thinking.machines.dmframework.dml;

import com.thinking.machines.dmframework.pojo.*;
import java.util.*;
import java.lang.reflect.*;
import com.thinking.machines.dmframework.dml.*;

public class DeleteWrapper {
    private String sqlStatement;
    private ArrayList<MethodWrapper> getterMethods;
    private ArrayList<Method> preparedStatementSetterMethods;

    public DeleteWrapper(String sqlStatement, ArrayList<MethodWrapper> getterMethods,
            ArrayList<Method> preparedStatementSetterMethods) {
        this.sqlStatement = sqlStatement;
        this.getterMethods = getterMethods;
        this.preparedStatementSetterMethods = preparedStatementSetterMethods;
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
}