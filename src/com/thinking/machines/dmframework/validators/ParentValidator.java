package com.thinking.machines.dmframework.validators;

import org.apache.log4j.*;

import com.thinking.machines.dmframework.annotations.Table;
import com.thinking.machines.dmframework.exceptions.*;
import com.thinking.machines.dmframework.pojo.*;
import com.thinking.machines.dmframework.utilities.*;
import java.util.*;
import java.lang.reflect.*;
import java.sql.*;

public class ParentValidator implements KeyValidator {
    private final static Logger logger = Logger.getLogger(ParentValidator.class);
    private ArrayList<ParentWrapper> parentWrappers;

    public ParentValidator(ArrayList<ParentWrapper> parentWrappers) {
        this.parentWrappers = parentWrappers;
    }

    public void validate(Connection connection, Object object, boolean throwExceptionIfExists)
            throws DMFrameworkException, ValidatorException {
        for (ParentWrapper parentWrapper : parentWrappers) {
            validateParent(connection, object, throwExceptionIfExists, parentWrapper.getGetterMethods(),
                    parentWrapper.getPreparedStatementSetterMethods(),
                    parentWrapper.getExceptionMessage(), parentWrapper.getSQLStatement(),
                    parentWrapper.getParentTable(), parentWrapper.hasCompositeKey());
        }
    }

    private void validateParent(Connection connection, Object object, boolean throwExceptionIfExists,
            ArrayList<MethodWrapper> getterMethods, ArrayList<Method> preparedStatementSetterMethods,
            String exceptionMessage, String sqlStatement, Table parentTable, boolean hasCompositeKey)
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
                        validatorException.add("generic",
                                exceptionMessage + " exists against " + parentTable.getDisplayName()
                                        + ".");
                    } else {
                        validatorException.add(methodWrapper.getColumn().getProperty().getName(),
                                exceptionMessage + " exists against " + parentTable.getDisplayName() + ".");
                    }
                    // throw new DMFrameworkException(exceptionMessage+" exists against
                    // "+parentTable.getDisplayName()+".");
                }
            } else {
                if (!exists) {
                    if (hasCompositeKey) {
                        validatorException.add("generic",
                                exceptionMessage + " does not exist against " + parentTable.getDisplayName() + ".");
                    } else {
                        validatorException.add(methodWrapper.getColumn().getProperty().getName(),
                                exceptionMessage + "  does not exist against " + parentTable.getDisplayName() + ".");
                    }
                    // throw new DMFrameworkException(exceptionMessage+" does not exist against
                    // "+parentTable.getDisplayName()+".");
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
