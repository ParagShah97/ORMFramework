package com.thinking.machines.dmframework.validators;

import java.lang.reflect.*;
import java.util.*;
import com.thinking.machines.dmframework.pojo.*;

public class ParentWrapper implements java.io.Serializable {
    private Table parentTable;
    private Table childTable;
    private ArrayList<MethodWrapper> getterMethods;
    private ArrayList<Method> preparedStatementSetterMethods;
    private String exceptionMessage;
    private String sqlStatement;

    public ParentWrapper(ArrayList<MethodWrapper> getterMethods, ArrayList<Method> preparedStatementSetterMethods,
            String exceptionMessage, String sqlStatement, Table childTable, Table parentTable) {
        this.getterMethods = getterMethods;
        this.preparedStatementSetterMethods = preparedStatementSetterMethods;
        this.exceptionMessage = exceptionMessage;
        this.sqlStatement = sqlStatement;
        this.parentTable = parentTable;
        this.childTable = childTable;
    }

    public ArrayList<MethodWrapper> getGetterMethods() {
        return this.getterMethods;
    }

    public ArrayList<Method> getPreparedStatementSetterMethods() {
        return this.preparedStatementSetterMethods;
    }

    public String getExceptionMessage() {
        return this.exceptionMessage;
    }

    public String getSQLStatement() {
        return this.sqlStatement;
    }

    public Table getParentTable() {
        return this.parentTable;
    }

    public Table getChildTable() {
        return this.childTable;
    }

    public boolean hasCompositeKey() {
        return this.getterMethods.size() > 1;
    }
}