package com.thinking.machines.dmframework.validators;

import org.apache.log4j.*;

import com.thinking.machines.dmframework.annotations.Table;
import com.thinking.machines.dmframework.exceptions.*;
import com.thinking.machines.dmframework.pojo.*;
import com.thinking.machines.dmframework.utilities.*;
import java.util.*;
import java.lang.reflect.*;
import java.sql.*;

public class ChildValidator implements KeyValidator {
    private final static Logger logger = Logger.getLogger(ChildValidator.class);
    private ArrayList<ChildWrapper> childWrappers;

    public ChildValidator(ArrayList<ChildWrapper> childWrappers) {
        this.childWrappers = childWrappers;
    }

    public void validate(Connection connection, Object object, boolean throwExceptionIfExists)
            throws DMFrameworkException, ValidatorException {
        for (ChildWrapper childWrapper : childWrappers) {
            validateChild(connection, object, throwExceptionIfExists, childWrapper.getGetterMethods(),
                    childWrapper.getPreparedStatementSetterMethods(), childWrapper.getExceptionMessage(),
                    childWrapper.getSQLStatement(), childWrapper.getChildTable(), childWrapper.hasCompositeKey());
        }
    }

    private void validateChild(Connection connection, Object object, boolean throwExceptionIfExists,
            ArrayList<MethodWrapper> getterMethods, ArrayList<Method> preparedStatementSetterMethods,
            String exceptionMessage, String sqlStatement, Table childTable, boolean hasCompositeKey)
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
                                exceptionMessage + " exists against " + childTable.getDisplayName()
                                        + ".");
                    } else {
                        validatorException.add(methodWrapper.getColumn().getProperty().getName(),
                                exceptionMessage + " exists against " + childTable.getDisplayName() + ".");
                    }
                    // throw new DMFrameworkException(exceptionMessage+" exists against
                    // "+childTable.getDisplayName()+".");
                }
            } else {
                if (!exists) {
                    if (hasCompositeKey) {
                        validatorException.add("generic",
                                exceptionMessage + " does not exist against " + childTable.getDisplayName() + ".");
                    } else {
                        validatorException.add(methodWrapper.getColumn().getProperty().getName(),
                                exceptionMessage + " does not exist against " + childTable.getDisplayName() + ".");
                    }
                    // throw new DMFrameworkException(exceptionMessage+" does not exist against
                    // "+childTable.getDisplayName()+".");
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

    public void validateByPrimaryKey(Connection connection, boolean throwExceptionIfExists, Object... primaryKey)
            throws DMFrameworkException, ValidatorException {
        for (ChildWrapper childWrapper : childWrappers) {
            validateChildByPrimaryKey(connection, throwExceptionIfExists,
                    childWrapper.getPreparedStatementSetterMethods(), childWrapper.getExceptionMessage(),
                    childWrapper.getSQLStatement(), childWrapper.getChildTable(), childWrapper.getParentTable(),
                    primaryKey);
        }
    }

    private void validateChildByPrimaryKey(Connection connection, boolean throwExceptionIfExists,
            ArrayList<Method> preparedStatementSetterMethods, String exceptionMessage, String sqlStatement,
            Table childTable, Table parentTable, Object... primaryKey)
            throws DMFrameworkException, ValidatorException {
        ValidatorException validatorException = new ValidatorException();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            boolean exists = false;
            preparedStatement = connection.prepareStatement(sqlStatement);
            int questionMarkPosition = 1;
            Method preparedStatementSetterMethod;
            for (int k = 0; k < preparedStatementSetterMethods.size(); k++) {
                preparedStatementSetterMethod = preparedStatementSetterMethods.get(k);
                try {
                    preparedStatementSetterMethod.invoke(preparedStatement, questionMarkPosition, primaryKey[k]);
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
                    if (primaryKey.length > 0) {
                        validatorException.add("generic",
                                exceptionMessage + " exists against " + childTable.getDisplayName()
                                        + ".");
                    } else {
                        validatorException.add(
                                parentTable.getPrimaryKeyColumn(parentTable.getPrimaryKeys().get(0)).getProperty()
                                        .getName(),
                                exceptionMessage + " exists against " + childTable.getDisplayName() + ".");
                    }
                    // throw new DMFrameworkException(exceptionMessage+" exists against
                    // "+childTable.getDisplayName()+".");
                }
            } else {
                if (!exists) {
                    validatorException.add("generic",
                            exceptionMessage + " does not exist against " + childTable.getDisplayName() + ".");
                    // throw new DMFrameworkException(exceptionMessage+" does not exist against
                    // "+childTable.getDisplayName()+".");
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
