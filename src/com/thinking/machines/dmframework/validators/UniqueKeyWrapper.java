package com.thinking.machines.dmframework.validators;

import java.lang.reflect.*;
import java.util.*;
import com.thinking.machines.dmframework.pojo.*;

public class UniqueKeyWrapper implements java.io.Serializable {
    private boolean isForUpdateOperation;
    private ArrayList<MethodWrapper> getterMethods;
    private ArrayList<Method> preparedStatementSetterMethods;
    private String exceptionMessage;
    private String sqlStatement;
    private int numberOfPrimaryFields;

    public UniqueKeyWrapper(ArrayList<MethodWrapper> getterMethods, ArrayList<Method> preparedStatementSetterMethods,
            String exceptionMessage, String sqlStatement) {
        this.getterMethods = getterMethods;
        this.preparedStatementSetterMethods = preparedStatementSetterMethods;
        this.exceptionMessage = exceptionMessage;
        this.sqlStatement = sqlStatement;
        this.isForUpdateOperation = false;
        this.numberOfPrimaryFields = 0;
    }

    public UniqueKeyWrapper(ArrayList<MethodWrapper> getterMethods, ArrayList<Method> preparedStatementSetterMethods,
            String exceptionMessage, String sqlStatement, boolean isForUpdateOperation, int numberOfPrimaryFields) {
        this.getterMethods = getterMethods;
        this.preparedStatementSetterMethods = preparedStatementSetterMethods;
        this.exceptionMessage = exceptionMessage;
        this.sqlStatement = sqlStatement;
        this.isForUpdateOperation = isForUpdateOperation;
        this.numberOfPrimaryFields = numberOfPrimaryFields;
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

    public boolean hasCompositeKey() {
        if (isForUpdateOperation == false) {
            return getterMethods.size() > 1;
        }
        return (getterMethods.size() - numberOfPrimaryFields) > 1;
    }
}