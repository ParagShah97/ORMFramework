package com.thinking.machines.dmframework.validators;

import org.apache.log4j.*;
import com.thinking.machines.dmframework.exceptions.*;
import com.thinking.machines.dmframework.pojo.*;
import java.util.*;
import java.lang.reflect.*;
import java.sql.*;

public class PrimaryKeyValidator implements KeyValidator {
    private final static Logger logger = Logger.getLogger(PrimaryKeyValidator.class);

    private List<MethodWrapper> getterMethods;
    private List<Method> preparedStatementSetterMethods;
    private String exceptionMessage;
    private String sqlStatement;

    public PrimaryKeyValidator(String sqlStatement, ArrayList<MethodWrapper> getterMethods,
            ArrayList<Method> preparedStatementSetterMethods, String exceptionMessage) {
        this.getterMethods = getterMethods;
        this.sqlStatement = sqlStatement;
        this.preparedStatementSetterMethods = preparedStatementSetterMethods;
        this.exceptionMessage = exceptionMessage;
    }

    public void validate(Connection connection, Object object, boolean throwExceptionIfExists)
            throws DMFrameworkException, ValidatorException {
        ValidatorException validatorException = new ValidatorException();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            preparedStatement = connection.prepareStatement(sqlStatement);
            int questionMarkPosition = 1;

            for (int k = 0; k < getterMethods.size(); k++) {
                MethodWrapper methodWrapper = getterMethods.get(k);
                Method setterMethod = preparedStatementSetterMethods.get(k);
                Object value;

                try {
                    value = methodWrapper.invoke(object);
                    setterMethod.invoke(preparedStatement, questionMarkPosition, value);
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                    logger.error("Error setting value in prepared statement: " + ex.getMessage(), ex);
                    throw new DMFrameworkException("Error setting value in prepared statement: " + ex.getMessage());
                }

                questionMarkPosition++;
            }

            resultSet = preparedStatement.executeQuery();
            boolean exists = resultSet.next();

            if (throwExceptionIfExists) {
                if (exists) {
                    if (getterMethods.size() > 1) {
                        validatorException.add("generic", exceptionMessage + " exists.");
                    } else {
                        validatorException.add(getterMethods.get(0).getColumn().getProperty().getName(),
                                exceptionMessage + " exists.");
                    }
                }
            } else {
                if (!exists) {
                    if (getterMethods.size() > 1) {
                        validatorException.add("generic", exceptionMessage + " does not exist.");
                    } else {
                        validatorException.add(getterMethods.get(0).getColumn().getProperty().getName(),
                                exceptionMessage + " does not exist.");
                    }
                }
            }

        } catch (SQLException sqlException) {
            logger.error("SQL error during validation: " + sqlException.getMessage(), sqlException);
            throw new DMFrameworkException("SQL error: " + sqlException.getMessage());
        } finally {
            try {
                if (resultSet != null)
                    resultSet.close();
                if (preparedStatement != null)
                    preparedStatement.close();
            } catch (SQLException closeException) {
                logger.error("Error closing resources: " + closeException.getMessage(), closeException);
            }
        }

        if (validatorException.hasExceptions())
            throw validatorException;
    }
}
