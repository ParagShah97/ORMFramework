package com.thinking.machines.dmframework.validators;

import org.apache.log4j.*;
import com.thinking.machines.dmframework.exceptions.*;
import com.thinking.machines.dmframework.pojo.*;
import com.thinking.machines.dmframework.utilities.*;
import java.util.*;
import java.lang.reflect.*;
import java.sql.*;

/**
 * Validates required fields and performs duplicate existence checks using reflection and JDBC.
 */
public class RequiredValidator implements Validator {
    private final static Logger logger = Logger.getLogger(RequiredValidator.class);
    private ArrayList<MethodWrapper> getterMethods;
    private ArrayList<MethodWrapper> setterMethods;
    private ArrayList<Method> preparedStatementSetterMethods;
    private String sqlStatement;
    private String exceptionMessage;

    public RequiredValidator(ArrayList<MethodWrapper> setterMethods, ArrayList<MethodWrapper> getterMethods) {
        this.setterMethods = setterMethods;
        this.getterMethods = getterMethods;
    }

    /**
     * Checks if an object is null or an empty string.
     */
    private boolean isEmpty(Object object) {
        if (object == null) return true;
        if (object instanceof String) return ((String) object).trim().isEmpty();
        return false;
    }

    /**
     * Validates that all required properties are non-empty.
     * If a default value is defined, it will set it.
     */
    public void validate(Object object) throws DMFrameworkException, ValidatorException {
        ValidatorException validatorException = new ValidatorException();
        for (int i = 0; i < getterMethods.size(); i++) {
            MethodWrapper getter = getterMethods.get(i);
            try {
                if (isEmpty(getter.invoke(object))) {
                    if (getter.getColumn().getDefaultValue() == null) {
                        validatorException.add(getter.getColumn().getProperty().getName(),
                                getter.getCapitalizedSpacedProperty() + " required.");
                    } else {
                        MethodWrapper setter = setterMethods.get(i);
                        try {
                            setter.invoke(object, setter.getColumn().getDefaultValue());
                        } catch (Exception e) {
                            validatorException.add(getter.getColumn().getProperty().getName(),
                                    getter.getCapitalizedSpacedProperty() + " required.");
                        }
                    }
                }
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                logger.error(e.getMessage());
                throw new DMFrameworkException(e.getMessage());
            }
        }
        if (validatorException.hasExceptions()) throw validatorException;
    }

    /**
     * Validates before insertion:
     * - Skips auto-increment fields
     * - Applies default values if defined
     * - Adds errors for required fields missing
     */
    public void validateBeforeInsertion(Object object) throws DMFrameworkException, ValidatorException {
        ValidatorException validatorException = new ValidatorException();
        for (int i = 0; i < getterMethods.size(); i++) {
            MethodWrapper getter = getterMethods.get(i);
            if (getter.getColumn().getIsAutoIncrementEnabled()) continue;

            try {
                if (isEmpty(getter.invoke(object))) {
                    if (getter.getColumn().getDefaultValue() == null) {
                        validatorException.add(getter.getColumn().getProperty().getName(),
                                getter.getCapitalizedSpacedProperty() + " required.");
                    } else {
                        MethodWrapper setter = setterMethods.get(i);
                        try {
                            setter.invoke(object, setter.getColumn().getDefaultValue());
                        } catch (Exception e) {
                            validatorException.add(getter.getColumn().getProperty().getName(),
                                    getter.getCapitalizedSpacedProperty() + " required.");
                        }
                    }
                }
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                logger.error(e.getMessage());
                throw new DMFrameworkException(e.getMessage());
            }
        }
        if (validatorException.hasExceptions()) throw validatorException;
    }

    /**
     * Checks for existence (or non-existence) of a record in the database using composite keys.
     * Skips check if any column is auto-incremented.
     *
     * @param connection JDBC connection
     * @param object Object to validate
     * @param throwExceptionIfExists If true, throws if record exists; otherwise, throws if not exists
     */
    public void validateBeforeInsertion(Connection connection, Object object, boolean throwExceptionIfExists)
            throws DMFrameworkException, ValidatorException {
        ValidatorException validatorException = new ValidatorException();
        
        // Skip validation if any composite key is auto-incremented
        for (MethodWrapper methodWrapper : getterMethods) {
            if (methodWrapper.getColumn().getIsAutoIncrementEnabled()) return;
        }

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            boolean exists;
            preparedStatement = connection.prepareStatement(sqlStatement);
            int position = 1;

            // Dynamically bind parameters using reflection
            for (int i = 0; i < getterMethods.size(); i++) {
                Method setter = preparedStatementSetterMethods.get(i);
                setter.invoke(preparedStatement, position++, getterMethods.get(i).invoke(object));
            }

            resultSet = preparedStatement.executeQuery();
            exists = resultSet.next();

            // Conditionally raise exceptions based on result
            if (throwExceptionIfExists && exists) {
                String field = getterMethods.size() > 1 ? "generic" : getterMethods.get(0).getColumn().getProperty().getName();
                validatorException.add(field, exceptionMessage + " exists.");
            } else if (!throwExceptionIfExists && !exists) {
                String field = getterMethods.size() > 1 ? "generic" : getterMethods.get(0).getColumn().getProperty().getName();
                validatorException.add(field, exceptionMessage + " does not exist.");
            }
        } catch (SQLException | IllegalAccessException | InvocationTargetException e) {
            logger.error(e.getMessage());
            throw new DMFrameworkException(e.getMessage());
        } finally {
            // Cleanup resources
            try {
                if (resultSet != null && !resultSet.isClosed()) resultSet.close();
                if (preparedStatement != null && !preparedStatement.isClosed()) preparedStatement.close();
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }
        }

        if (validatorException.hasExceptions()) throw validatorException;
    }

    /**
     * Checks if a record exists in the DB based on primary key values.
     * Works similar to `validateBeforeInsertion`, but takes values directly.
     */
    public void validateByPrimaryKey(Connection connection, boolean throwExceptionIfExists, Object... primaryKey)
            throws DMFrameworkException, ValidatorException {
        ValidatorException validatorException = new ValidatorException();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            boolean exists;
            preparedStatement = connection.prepareStatement(sqlStatement);

            // Dynamically bind primary key values
            for (int i = 0; i < getterMethods.size(); i++) {
                Method setter = preparedStatementSetterMethods.get(i);
                setter.invoke(preparedStatement, i + 1, primaryKey[i]);
            }

            resultSet = preparedStatement.executeQuery();
            exists = resultSet.next();

            // Raise exception if the condition is violated
            if (throwExceptionIfExists && exists) {
                String field = getterMethods.size() > 1 ? "generic" : getterMethods.get(0).getColumn().getProperty().getName();
                validatorException.add(field, exceptionMessage + " exists.");
            } else if (!throwExceptionIfExists && !exists) {
                String field = getterMethods.size() > 1 ? "generic" : getterMethods.get(0).getColumn().getProperty().getName();
                validatorException.add(field, exceptionMessage + " does not exist.");
            }
        } catch (SQLException | IllegalAccessException | InvocationTargetException e) {
            logger.error(e.getMessage());
            throw new DMFrameworkException(e.getMessage());
        } finally {
            // Ensure resource cleanup
            try {
                if (resultSet != null && !resultSet.isClosed()) resultSet.close();
                if (preparedStatement != null && !preparedStatement.isClosed()) preparedStatement.close();
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }
        }

        if (validatorException.hasExceptions()) throw validatorException;
    }

    // Setter methods for injection of SQL metadata
    public void setSqlStatement(String sqlStatement) {
        this.sqlStatement = sqlStatement;
    }

    public void setPreparedStatementSetterMethods(ArrayList<Method> preparedStatementSetterMethods) {
        this.preparedStatementSetterMethods = preparedStatementSetterMethods;
    }

    public void setExceptionMessage(String exceptionMessage) {
        this.exceptionMessage = exceptionMessage;
    }
}
