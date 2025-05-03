package com.thinking.machines.dmframework;

import com.thinking.machines.dmframework.pojo.*;
import com.thinking.machines.dmframework.utilities.*;
import com.thinking.machines.dmframework.exceptions.*;
import com.thinking.machines.dmframework.query.*;
import com.thinking.machines.dmframework.dml.*;
import java.util.*;
import org.apache.log4j.*;
import java.sql.*;
import java.lang.reflect.*;
import java.lang.annotation.*;

public class DataManager {
    private static Database database = null;
    private Connection connection;
    final public static Logger logger = Logger.getLogger(DataManager.class);
    static {
        try {
            database = DatabaseUtility.getDatabase(ConfigurationUtility.getDatabaseConfiguration(),
                    ConfigurationUtility.getTablesToIgnore(), ConfigurationUtility.getViewsToIgnore());
        } catch (ClassNotFoundException classNotFoundException) {
            logger.fatal("Configuration file TMDMFramework.xml contains wrong class name : "
                    + classNotFoundException.getMessage());
            throw new RuntimeException("Configuration file TMDMFramework.xml contains wrong class name : "
                    + classNotFoundException.getMessage());
        } catch (DMFrameworkException dmFrameworkException) {
            String alternatePath = DataManager.class.getResource("/").toString();
            alternatePath = alternatePath.toString().substring(5);
            ConfigurationUtility.configurationFilePath = alternatePath + "TMDMFramework.xml";
            try {
                database = DatabaseUtility.getDatabase(ConfigurationUtility.getDatabaseConfiguration(),
                        ConfigurationUtility.getTablesToIgnore(), ConfigurationUtility.getViewsToIgnore());
            } catch (ClassNotFoundException classNotFoundException) {
                logger.fatal("Configuration file TMDMFramework.xml contains wrong class name : "
                        + classNotFoundException.getMessage());
                throw new RuntimeException("Configuration file TMDMFramework.xml contains wrong class name : "
                        + classNotFoundException.getMessage());
            } catch (DMFrameworkException dmFrameworkException2) {
                logger.fatal(dmFrameworkException2.getMessage());
                throw new RuntimeException(dmFrameworkException2.getMessage());
            }
        }
    }

    public DataManager() {
    }

    public <T> Select<T> select(Class<T> entityClass) throws DMFrameworkException {
        if (connection == null)
            throw new DMFrameworkException("Call to begin of DataManager is missing");
        try {
            Entity entity = EntityManager.get(entityClass);
            return entity.<T>select(connection);
        } catch (DMFrameworkException dmFrameworkException) {
            try {
                if (this.connection != null) {
                    this.connection.close();
                    this.connection = null;
                }
            } catch (SQLException sqlException1) {
            }
            dmFrameworkException
                    .setDatabaseOperationException("Unable to apply select on " + entityClass.getSimpleName());
            throw dmFrameworkException;
        }
    }

    public void insert(Object object) throws DMFrameworkException, ValidatorException {
        if (connection == null)
            throw new DMFrameworkException("Call to begin of DataManager is missing");
        Class entityClass = object.getClass();
        LinkedList<Pair<Object, MethodWrapper>> oldValues = null;
        try {
            Entity entity = EntityManager.get(entityClass);
            HashMap<Integer, SequenceWrapper> sequenceWrappers;
            sequenceWrappers = entity.getSequenceWrappers();
            if (sequenceWrappers != null && sequenceWrappers.size() > 0) {
                oldValues = new LinkedList<Pair<Object, MethodWrapper>>();
                SequenceWrapper sequenceWrapper;
                String nextValSQLStatement;
                MethodWrapper nextValObjectPropertyGetter;
                MethodWrapper nextValObjectPropertySetter;
                Method nextValResultSetGetter;
                Object oldValue;
                Statement nextValStatement;
                ResultSet nextValResultSet;
                for (int i = 0; i < entity.getNumberOfValuesInInsert(); i++) {
                    sequenceWrapper = sequenceWrappers.get(new Integer(i + 1));
                    if (sequenceWrapper != null) {
                        nextValObjectPropertyGetter = sequenceWrapper.getNextValObjectPropertyGetter();
                        nextValObjectPropertySetter = sequenceWrapper.getNextValObjectPropertySetter();
                        nextValResultSetGetter = sequenceWrapper.getNextValResultSetGetter();
                        try {
                            nextValStatement = connection.createStatement();
                            nextValResultSet = nextValStatement.executeQuery(sequenceWrapper.getNextValSQLStatement());
                            nextValResultSet.next();
                        } catch (SQLException sqlException) {
                            try {
                                if (this.connection != null) {
                                    this.connection.close();
                                    this.connection = null;
                                }
                            } catch (SQLException sqlException1) {
                            }
                            try {
                                for (Pair<Object, MethodWrapper> nextValResetPair : oldValues) {
                                    nextValResetPair.getSecond().invoke(object, nextValResetPair.getFirst());
                                }
                            } catch (Exception exception) {
                                // ignore, cannot do anything about it
                            }
                            logger.fatal(sqlException.getMessage() + " in case of insert operation for "
                                    + entityClass.getSimpleName());
                            throw new DMFrameworkException(sqlException.getMessage()
                                    + " in case of insert operation for " + entityClass.getSimpleName());
                        }
                        try {
                            oldValue = nextValObjectPropertyGetter.invoke(object);
                            oldValues.add(new Pair<Object, MethodWrapper>(oldValue, nextValObjectPropertySetter));
                            nextValObjectPropertySetter.invoke(object,
                                    nextValResultSetGetter.invoke(nextValResultSet, 1));
                        } catch (IllegalAccessException | IllegalArgumentException
                                | InvocationTargetException commonException) {
                            logger.fatal(commonException.getMessage() + " in case of insert operation for "
                                    + entityClass.getSimpleName());
                            DMFrameworkException dmFrameworkException = new DMFrameworkException(
                                    commonException.getMessage() + " in case of insert operation for "
                                            + entityClass.getSimpleName());
                            dmFrameworkException
                                    .setDatabaseOperationException("Cannot add " + entityClass.getSimpleName());
                            throw dmFrameworkException;
                        }
                        try {
                            nextValResultSet.close();
                            nextValStatement.close();
                        } catch (SQLException sqlException) {
                            // ignore
                        }
                    } // if sequenceWrapper!=null
                } // loop for number of values in insert statement ends
            }
            entity.validateConstraintsBeforeInsertion(connection, object);
            entity.insert(connection, object);
        } catch (ValidatorException validatorException) {
            try {
                if (this.connection != null) {
                    this.connection.close();
                    this.connection = null;
                }
            } catch (SQLException sqlException1) {
            }
            // lot of work to be done
            validatorException.setDatabaseOperationException("Cannot add " + entityClass.getSimpleName());
            throw validatorException;
        } catch (DMFrameworkException dmFrameworkException) {
            try {
                if (this.connection != null) {
                    this.connection.close();
                    this.connection = null;
                }
            } catch (SQLException sqlException1) {
            }
            if (oldValues != null) {
                try {
                    for (Pair<Object, MethodWrapper> nextValResetPair : oldValues) {
                        nextValResetPair.getSecond().invoke(object, nextValResetPair.getFirst());
                    }
                } catch (Exception exception) {
                    // ignore, cannot do anything about it
                }
            }
            dmFrameworkException.setDatabaseOperationException("Cannot add " + entityClass.getSimpleName());
            throw dmFrameworkException;
        }
    }

    public void update(Object object) throws DMFrameworkException, ValidatorException {
        if (connection == null)
            throw new DMFrameworkException("Call to begin of DataManager is missing");
        Class entityClass = object.getClass();
        try {
            Entity entity = EntityManager.get(entityClass);
            entity.validateConstraintsBeforeUpdation(connection, object);
            entity.update(connection, object);
        } catch (ValidatorException validatorException) {
            try {
                if (this.connection != null) {
                    this.connection.close();
                    this.connection = null;
                }
            } catch (SQLException sqlException1) {
            }
            // lot of work to be done
            validatorException.setDatabaseOperationException("Cannot update " + entityClass.getSimpleName());
            throw validatorException;
        } catch (DMFrameworkException dmFrameworkException) {
            try {
                if (this.connection != null) {
                    this.connection.close();
                    this.connection = null;
                }
            } catch (SQLException sqlException1) {
            }
            dmFrameworkException.setDatabaseOperationException("Cannot update " + entityClass.getSimpleName());
            throw dmFrameworkException;
        }
    }

    public void delete(Object object) throws DMFrameworkException, ValidatorException {
        if (connection == null)
            throw new DMFrameworkException("Call to begin of DataManager is missing");
        Class entityClass = object.getClass();
        try {
            Entity entity = EntityManager.get(entityClass);
            entity.validateConstraintsBeforeDeleting(connection, object);
            entity.delete(connection, object);
        } catch (ValidatorException validatorException) {
            try {
                if (this.connection != null) {
                    this.connection.close();
                    this.connection = null;
                }
            } catch (SQLException sqlException1) {
            }
            // lot of work to be done
            validatorException.setDatabaseOperationException("Cannot delete : " + entityClass.getSimpleName());
            throw validatorException;
        } catch (DMFrameworkException dmFrameworkException) {
            try {
                if (this.connection != null) {
                    this.connection.close();
                    this.connection = null;
                }
            } catch (SQLException sqlException1) {
            }
            dmFrameworkException.setDatabaseOperationException("Cannot delete : " + entityClass.getSimpleName());
            throw dmFrameworkException;
        }
    }

    public void delete(Class entityClass, Object... primaryKey) throws DMFrameworkException, ValidatorException {
        if (connection == null)
            throw new DMFrameworkException("Call to begin of DataManager is missing");
        try {
            Entity entity = EntityManager.get(entityClass);
            entity.validateConstraintsByPrimaryKeyBeforeDeleting(connection, primaryKey);
            entity.deleteByPrimaryKey(connection, primaryKey);
        } catch (ValidatorException validatorException) {
            try {
                if (this.connection != null) {
                    this.connection.close();
                    this.connection = null;
                }
            } catch (SQLException sqlException1) {
            }
            // lot of work to be done
            validatorException.setDatabaseOperationException("Cannot delete : " + entityClass.getSimpleName());
            throw validatorException;
        } catch (DMFrameworkException dmFrameworkException) {
            try {
                if (this.connection != null) {
                    this.connection.close();
                    this.connection = null;
                }
            } catch (SQLException sqlException1) {
            }
            dmFrameworkException.setDatabaseOperationException("Cannot delete : " + entityClass.getSimpleName());
            throw dmFrameworkException;
        }
    }

    public Connection getConnection() {
        return this.connection;
    }

    public void begin() throws DMFrameworkException {
        connection = DatabaseUtility.getConnection();
        try {
            connection.setAutoCommit(false);
        } catch (SQLException sqlException) {
            logger.warn(sqlException.getMessage());
        }
    }

    public void end() {
        if (connection != null) {
            try {
                connection.commit();
            } catch (Exception exception) {
                logger.warn(exception.getMessage());
            }
            try {
                if (connection != null) {
                    connection.close();
                    connection = null;
                }
            } catch (Exception exception) {
                logger.warn(exception.getMessage());
            }
        }
    }
}