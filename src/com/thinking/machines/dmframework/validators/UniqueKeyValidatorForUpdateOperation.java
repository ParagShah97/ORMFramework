package com.thinking.machines.dmframework.validators;

import org.apache.log4j.*;
import com.thinking.machines.dmframework.exceptions.*;
import com.thinking.machines.dmframework.pojo.*;
import com.thinking.machines.dmframework.utilities.*;
import java.util.*;
import java.lang.reflect.*;
import java.sql.*;

public class UniqueKeyValidatorForUpdateOperation implements KeyValidator {
    private final static Logger logger = Logger.getLogger(UniqueKeyValidatorForUpdateOperation.class);
    private ArrayList<UniqueKeyWrapper> uniqueKeyWrappers;

    public UniqueKeyValidatorForUpdateOperation(ArrayList<UniqueKeyWrapper> uniqueKeyWrappers) {
        this.uniqueKeyWrappers = uniqueKeyWrappers;
    }

    public void validate(Connection connection, Object object, boolean throwExceptionIfExists)
            throws DMFrameworkException, ValidatorException {
        for (UniqueKeyWrapper uniqueKeyWrapper : uniqueKeyWrappers) {
            validateUniqueKey(connection, object, throwExceptionIfExists, uniqueKeyWrapper.getGetterMethods(),
                    uniqueKeyWrapper.getPreparedStatementSetterMethods(), uniqueKeyWrapper.getExceptionMessage(),
                    uniqueKeyWrapper.getSQLStatement(), uniqueKeyWrapper.hasCompositeKey());
        }
    }

    private void validateUniqueKey(Connection connection, Object object, boolean throwExceptionIfExists,
            ArrayList<MethodWrapper> getterMethods, ArrayList<Method> preparedStatementSetterMethods,
            String exceptionMessage, String sqlStatement, boolean hasCompositeKey)
            throws DMFrameworkException, ValidatorException {
        ValidatorException validatorException = new ValidatorException();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            boolean exists = false;
            preparedStatement = connection.prepareStatement(sqlStatement);
            int questionMarkPosition = 1;
            MethodWrapper methodWrapper = null;
            Method preparedStatementSetterMethod;
            for (int k = 0; k < getterMethods.size(); k++) {
                methodWrapper = getterMethods.get(k);
                preparedStatementSetterMethod = preparedStatementSetterMethods.get(k);
                try {
                    preparedStatementSetterMethod.invoke(preparedStatement, questionMarkPosition,
                            methodWrapper.invoke(object));
                } catch (IllegalAccessException | IllegalArgumentException
                        | InvocationTargetException commonException) {
                    logger.error(commonException.getMessage());
                    throw new DMFrameworkException(commonException.getMessage());
                }
                questionMarkPosition++;
            }
            resultSet = preparedStatement.executeQuery();
            exists = resultSet.next();
            resultSet.close();
            preparedStatement.close();
            if (throwExceptionIfExists) {
                if (exists) {
                    if (hasCompositeKey) {
                        validatorException.add("generic", exceptionMessage + " exists.");
                    } else {
                        validatorException.add(getterMethods.get(0).getColumn().getProperty().getName(),
                                exceptionMessage + " exists.");
                    }
                    // throw new DMFrameworkException(exceptionMessage+" exists.");
                }
            } else {
                if (!exists) {
                    if (hasCompositeKey) {
                        validatorException.add("generic", exceptionMessage + " does not exist.");
                    } else {
                        validatorException.add(getterMethods.get(0).getColumn().getProperty().getName(),
                                exceptionMessage
                                        + " does not exist.");
                    }
                    // throw new DMFrameworkException(exceptionMessage+" does not exist.");
                }
            }
        } catch (SQLException sqlException) {
            try {
                if (resultSet != null && resultSet.isClosed() == false)
                    resultSet.close();
            } catch (SQLException se) {
            }
            try {
                if (preparedStatement != null && preparedStatement.isClosed() == false)
                    preparedStatement.close();
            } catch (SQLException se) {
            }
            logger.error(sqlException.getMessage());
            throw new DMFrameworkException(sqlException.getMessage());
        }
        if (validatorException.hasExceptions())
            throw validatorException;
    }
}