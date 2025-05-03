package com.thinking.machines.dmframework;

import com.thinking.machines.dmframework.exceptions.*;
import com.thinking.machines.dmframework.validators.*;
import com.thinking.machines.dmframework.dml.*;
import com.thinking.machines.dmframework.query.*;
import com.thinking.machines.dmframework.pojo.*;
import java.sql.*;
import java.util.*;
import java.lang.reflect.*;
import org.apache.log4j.*;

public class Entity {
    private Logger logger = Logger.getLogger(Entity.class);
    private HashMap<Integer, Pair<Field, Column>> fieldColumnHashMap;
    private HashMap<Integer, Pair<MethodWrapper, MethodWrapper>> setterGetterMethodHashMap;
    private Validator requiredValidator;
    private Validator overflowValidator;
    private KeyValidator primaryKeyValidator;
    private KeyValidator uniqueKeyValidator;
    private KeyValidator uniqueKeyValidatorForUpdateOperation;
    private KeyValidator parentValidator;
    private KeyValidator childValidator;
    private InsertWrapper insertWrapper;
    private UpdateWrapper updateWrapper;
    private DeleteWrapper deleteWrapper;
    private SelectWrapper selectWrapper;
    private Table table;
    private View view;
    private Class entityClass;
    private String displayName;

    Entity(Class entityClass) {
        this.entityClass = entityClass;
    }

    public HashMap<Integer, SequenceWrapper> getSequenceWrappers() {
        return this.insertWrapper.getSequenceWrappers();
    }

    public int getNumberOfValuesInInsert() {
        return this.insertWrapper.getPreparedStatementSetterMethods().size();
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return this.displayName;
    }

    public Class getEntityClass() {
        return this.entityClass;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return this.table;
    }

    public void setView(View view) {
        this.view = view;
    }

    public View getView() {
        return this.view;
    }

    public boolean isViewEntity() {
        return this.view != null;
    }

    public boolean isTableEntity() {
        return this.table != null;
    }

    public void setFieldColumnHashMap(HashMap<Integer, Pair<Field, Column>> fieldColumnHashMap) {
        this.fieldColumnHashMap = fieldColumnHashMap;
    }

    public void setSetterGetterMethodHashMap(
            HashMap<Integer, Pair<MethodWrapper, MethodWrapper>> setterGetterMethodHashMap) {
        this.setterGetterMethodHashMap = setterGetterMethodHashMap;
    }

    public void setSelectWrapper(SelectWrapper selectWrapper) {
        this.selectWrapper = selectWrapper;
    }

    public SelectWrapper getSelectWrapper() {
        return this.selectWrapper;
    }

    public void setRequiredValidator(Validator requiredValidator) {
        this.requiredValidator = requiredValidator;
    }

    public void setOverflowValidator(Validator overflowValidator) {
        this.overflowValidator = overflowValidator;
    }

    public void setPrimaryKeyValidator(KeyValidator primaryKeyValidator) {
        this.primaryKeyValidator = primaryKeyValidator;
    }

    public void setUniqueKeyValidator(KeyValidator uniqueKeyValidator) {
        this.uniqueKeyValidator = uniqueKeyValidator;
    }

    public void setUniqueKeyValidatorForUpdateOperation(KeyValidator uniqueKeyValidatorForUpdateOperation) {
        this.uniqueKeyValidatorForUpdateOperation = uniqueKeyValidatorForUpdateOperation;
    }

    public void setParentValidator(KeyValidator parentValidator) {
        this.parentValidator = parentValidator;
    }

    public void setChildValidator(KeyValidator childValidator) {
        this.childValidator = childValidator;
    }

    public void setInsertWrapper(InsertWrapper insertWrapper) {
        this.insertWrapper = insertWrapper;
    }

    public void setUpdateWrapper(UpdateWrapper updateWrapper) {
        this.updateWrapper = updateWrapper;
    }

    public void setDeleteWrapper(DeleteWrapper deleteWrapper) {
        this.deleteWrapper = deleteWrapper;
    }

    void validateConstraintsBeforeInsertion(Connection connection, Object object)
            throws DMFrameworkException, ValidatorException {
        requiredValidator.validateBeforeInsertion(object);
        // check for max width and precision
        overflowValidator.validate(object);
        // check for primary key existence
        primaryKeyValidator.validateBeforeInsertion(connection, object, true); // true :- raise exception if exists
        // check for unique key existence
        uniqueKeyValidator.validateBeforeInsertion(connection, object, true); // true :- raise exception if exists
        // check for parent's existence
        parentValidator.validate(connection, object, false); // false :- raise exception if does not exist
    }

    public <T> Select<T> select(Connection connection) throws DMFrameworkException {
        Select<T> select = new Select<T>(connection, this);
        return select;
    }

    void insert(Connection connection, Object object) throws DMFrameworkException {
        if (insertWrapper == null)
            throw new DMFrameworkException("Insert operation not supported on " + entityClass.getName());
        String sqlStatement;
        ArrayList<MethodWrapper> getterMethods;
        ArrayList<Method> preparedStatementSetterMethods;
        MethodWrapper autoIncrementedPropertySetter;
        Method autoIncrementedPropertyResultSetGetter;
        sqlStatement = insertWrapper.getSQLStatement();
        getterMethods = insertWrapper.getGetterMethods();
        preparedStatementSetterMethods = insertWrapper.getPreparedStatementSetterMethods();
        autoIncrementedPropertySetter = insertWrapper.getAutoIncrementedPropertySetter();
        autoIncrementedPropertyResultSetGetter = insertWrapper.getAutoIncrementedPropertyResultSetGetter();
        PreparedStatement preparedStatement = null;
        try {
            if (autoIncrementedPropertyResultSetGetter != null) {
                preparedStatement = connection.prepareStatement(sqlStatement, Statement.RETURN_GENERATED_KEYS);
            } else {
                preparedStatement = connection.prepareStatement(sqlStatement);
            }
        } catch (SQLException sqlException) {
            logger.fatal(sqlException.getMessage() + " in case of insert operation for " + entityClass.getSimpleName());
            throw new DMFrameworkException(
                    sqlException.getMessage() + " in case of insert operation for " + entityClass.getSimpleName());
        }
        Method setterMethod;
        MethodWrapper getterMethodWrapper;
        try {
            for (int i = 0; i < preparedStatementSetterMethods.size(); i++) {
                setterMethod = preparedStatementSetterMethods.get(i);
                getterMethodWrapper = getterMethods.get(i);
                setterMethod.invoke(preparedStatement, i + 1, getterMethodWrapper.invoke(object));
            }
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException commonException) {
            logger.fatal(
                    commonException.getMessage() + " in case of insert operation for " + entityClass.getSimpleName());
            throw new DMFrameworkException(
                    commonException.getMessage() + " in case of insert operation for " + entityClass.getSimpleName());
        }
        try {
            preparedStatement.executeUpdate();
        } catch (SQLException sqlException) {
            try {
                preparedStatement.close();
            } catch (SQLException sqlException2) {
            }
            logger.fatal(sqlException.getMessage() + " in case of insert operation for " + entityClass.getSimpleName());
            throw new DMFrameworkException(
                    sqlException.getMessage() + " in case of insert operation for " + entityClass.getSimpleName());
        }
        if (autoIncrementedPropertyResultSetGetter != null) {
            ResultSet resultSet = null;
            try {
                resultSet = preparedStatement.getGeneratedKeys();
            } catch (SQLException sqlException) {
                try {
                    preparedStatement.close();
                } catch (SQLException sqlException2) {
                }
                logger.fatal("Unable to fetch incremented key value of : " + autoIncrementedPropertySetter.getProperty()
                        + " of entity : " + entityClass.getSimpleName());
                throw new DMFrameworkException("Unable to fetch incremented key value of : "
                        + autoIncrementedPropertySetter.getProperty() + " of entity : " + entityClass.getSimpleName());
            }
            if (resultSet == null) {
                try {
                    preparedStatement.close();
                } catch (SQLException sqlException2) {
                }
                logger.fatal("Unable to fetch incremented key value of : " + autoIncrementedPropertySetter.getProperty()
                        + " of entity : " + entityClass.getSimpleName());
                throw new DMFrameworkException("Unable to fetch incremented key value of : "
                        + autoIncrementedPropertySetter.getProperty() + " of entity : " + entityClass.getSimpleName());
            }
            try {
                resultSet.next();
            } catch (SQLException sqlException) {
                try {
                    resultSet.close();
                } catch (SQLException sqlException2) {
                }
                try {
                    preparedStatement.close();
                } catch (SQLException sqlException3) {
                }
                logger.fatal("Unable to fetch incremented key value of : " + autoIncrementedPropertySetter.getProperty()
                        + " of entity : " + entityClass.getSimpleName());
                throw new DMFrameworkException("Unable to fetch incremented key value of : "
                        + autoIncrementedPropertySetter.getProperty() + " of entity : " + entityClass.getSimpleName());
            }
            try {
                autoIncrementedPropertySetter.invoke(object,
                        autoIncrementedPropertyResultSetGetter.invoke(resultSet, 1));
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException commonException) {
                logger.fatal(commonException.getMessage() + " in case of insert operation for "
                        + entityClass.getSimpleName());
                throw new DMFrameworkException(commonException.getMessage() + " in case of insert operation for "
                        + entityClass.getSimpleName());
            }
            try {
                resultSet.close();
            } catch (SQLException sqlException) {
            }
        }
        try {
            preparedStatement.close();
        } catch (SQLException sqlException) {
        }
    }

    void validateConstraintsBeforeUpdation(Connection connection, Object object)
            throws DMFrameworkException, ValidatorException {
        // check for null or zero length
        requiredValidator.validate(object);
        // check for max width and precision
        overflowValidator.validate(object);
        // check for primary key existence
        primaryKeyValidator.validate(connection, object, false); // false :- raise exception if does not exist
        // check for unique key existence
        uniqueKeyValidatorForUpdateOperation.validate(connection, object, true); // true :- raise exception if exists
        // check for parent's existence
        parentValidator.validate(connection, object, false); // false :- raise exception if does not exist
    }

    public void update(Connection connection, Object object) throws DMFrameworkException {
        if (updateWrapper == null)
            throw new DMFrameworkException("Update operation not supported on " + entityClass.getName());
        String sqlStatement;
        ArrayList<MethodWrapper> getterMethods;
        ArrayList<Method> preparedStatementSetterMethods;
        sqlStatement = updateWrapper.getSQLStatement();
        getterMethods = updateWrapper.getGetterMethods();
        preparedStatementSetterMethods = updateWrapper.getPreparedStatementSetterMethods();
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sqlStatement);
        } catch (SQLException sqlException) {
            logger.fatal(sqlException.getMessage() + " in case of update operation for " + entityClass.getSimpleName());
            throw new DMFrameworkException(
                    sqlException.getMessage() + " in case of update operation for " + entityClass.getSimpleName());
        }
        Method setterMethod;
        MethodWrapper getterMethodWrapper;
        try {
            for (int i = 0; i < preparedStatementSetterMethods.size(); i++) {
                setterMethod = preparedStatementSetterMethods.get(i);
                getterMethodWrapper = getterMethods.get(i);
                setterMethod.invoke(preparedStatement, i + 1, getterMethodWrapper.invoke(object));
            }
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException commonException) {
            logger.fatal(
                    commonException.getMessage() + " in case of update operation for " + entityClass.getSimpleName());
            throw new DMFrameworkException(
                    commonException.getMessage() + " in case of update operation for " + entityClass.getSimpleName());
        }
        try {
            preparedStatement.executeUpdate();
        } catch (SQLException sqlException) {
            try {
                preparedStatement.close();
            } catch (SQLException sqlException2) {
            }
            logger.fatal(
                    sqlException.getMessage() + " in case of update operation for  " + entityClass.getSimpleName());
            throw new DMFrameworkException(
                    sqlException.getMessage() + " in case of update operation for " + entityClass.getSimpleName());
        }
        try {
            preparedStatement.close();
        } catch (SQLException sqlException) {
        }
    }

    void validateConstraintsBeforeDeleting(Connection connection, Object object)
            throws DMFrameworkException, ValidatorException {
        // check for primary key existence
        primaryKeyValidator.validate(connection, object, false); // false :- raise exception if does not exist
        // check for child's existence
        childValidator.validate(connection, object, true); // true :- raise exception if exists
    }

    public void delete(Connection connection, Object object) throws DMFrameworkException {
        if (deleteWrapper == null)
            throw new DMFrameworkException("Delete operation not supported on " + entityClass.getName());
        String sqlStatement;
        ArrayList<MethodWrapper> getterMethods;
        ArrayList<Method> preparedStatementSetterMethods;
        sqlStatement = deleteWrapper.getSQLStatement();
        getterMethods = deleteWrapper.getGetterMethods();
        preparedStatementSetterMethods = deleteWrapper.getPreparedStatementSetterMethods();
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sqlStatement);
        } catch (SQLException sqlException) {
            logger.fatal(sqlException.getMessage() + " in case of delete operation for " + entityClass.getSimpleName());
            throw new DMFrameworkException(
                    sqlException.getMessage() + " in case of delete operation for " + entityClass.getSimpleName());
        }
        Method setterMethod;
        MethodWrapper getterMethodWrapper;
        try {
            for (int i = 0; i < preparedStatementSetterMethods.size(); i++) {
                setterMethod = preparedStatementSetterMethods.get(i);
                getterMethodWrapper = getterMethods.get(i);
                setterMethod.invoke(preparedStatement, i + 1, getterMethodWrapper.invoke(object));
            }
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException commonException) {
            logger.fatal(
                    commonException.getMessage() + " in case of delete operation for " + entityClass.getSimpleName());
            throw new DMFrameworkException(
                    commonException.getMessage() + " in case of delete operation for " + entityClass.getSimpleName());
        }
        try {
            preparedStatement.executeUpdate();
        } catch (SQLException sqlException) {
            try {
                preparedStatement.close();
            } catch (SQLException sqlException2) {
            }
            logger.fatal(sqlException.getMessage() + " in case of delete operation for " + entityClass.getSimpleName());
            throw new DMFrameworkException(
                    sqlException.getMessage() + " in case of delete operation for " + entityClass.getSimpleName());
        }
        try {
            preparedStatement.close();
        } catch (SQLException sqlException) {
        }
    }

    void validateConstraintsByPrimaryKeyBeforeDeleting(Connection connection, Object... primaryKey)
            throws DMFrameworkException, ValidatorException {
        // check for primary key existence
        primaryKeyValidator.validateByPrimaryKey(connection, false, primaryKey); // false :- raise exception if does not
                                                                                 // exist
        // check for child's existence
        childValidator.validateByPrimaryKey(connection, true, primaryKey); // true :- raise exception if exists
    }

    public void deleteByPrimaryKey(Connection connection, Object... primaryKey) throws DMFrameworkException {
        if (deleteWrapper == null)
            throw new DMFrameworkException("Delete operation not supported on    " + entityClass.getName());
        String sqlStatement;
        ArrayList<Method> preparedStatementSetterMethods;
        sqlStatement = deleteWrapper.getSQLStatement();
        preparedStatementSetterMethods = deleteWrapper.getPreparedStatementSetterMethods();
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sqlStatement);
        } catch (SQLException sqlException) {
            logger.fatal(
                    sqlException.getMessage() + " in case of delete operation for    " + entityClass.getSimpleName());
            throw new DMFrameworkException(
                    sqlException.getMessage() + " in case of delete operation for    " + entityClass.getSimpleName());
        }
        Method setterMethod;
        try {
            for (int i = 0; i < preparedStatementSetterMethods.size(); i++) {
                setterMethod = preparedStatementSetterMethods.get(i);
                setterMethod.invoke(preparedStatement, i + 1, primaryKey[i]);
            }
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException commonException) {
            logger.fatal(
                    commonException.getMessage() + " in case of delete operation for " + entityClass.getSimpleName());
            throw new DMFrameworkException(
                    commonException.getMessage() + " in case of delete operation for " + entityClass.getSimpleName());
        }
        try {
            preparedStatement.executeUpdate();
        } catch (SQLException sqlException) {
            try {
                preparedStatement.close();
            } catch (SQLException sqlException2) {
            }
            logger.fatal(sqlException.getMessage() + " in case of delete operation for " + entityClass.getSimpleName());
            throw new DMFrameworkException(
                    sqlException.getMessage() + " in case of delete operation for " + entityClass.getSimpleName());
        }
        try {
            preparedStatement.close();
        } catch (SQLException sqlException) {
        }
    }
}
