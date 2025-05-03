package com.thinking.machines.dmframework;

import com.thinking.machines.dmframework.validators.*;
import com.thinking.machines.dmframework.pojo.*;
import com.thinking.machines.dmframework.exceptions.*;
import com.thinking.machines.dmframework.utilities.*;
import com.thinking.machines.dmframework.dml.*;
import com.thinking.machines.dmframework.query.*;
import java.util.*;
import org.apache.log4j.*;
import java.lang.reflect.*;
import java.lang.annotation.*;

public class EntityManager {
    private static final HashMap<Class, Entity> entities = new HashMap<Class, Entity>();
    private static Database database = null;
    final public static Logger logger = Logger.getLogger(EntityManager.class);
    static {
        try {
            database = DatabaseUtility.getDatabase(ConfigurationUtility.getDatabaseConfiguration(),
                    ConfigurationUtility.getTablesToIgnore(), ConfigurationUtility.getViewsToIgnore());
            List<Class> listOfEntities = ConfigurationUtility.getEntities();
            Entity entity;
            com.thinking.machines.dmframework.annotations.Table tableAnnotation = null;
            com.thinking.machines.dmframework.annotations.View viewAnnotation = null;
            for (Class entityClass : listOfEntities) {
                entity = new Entity(entityClass);
                populate(entity);
                entities.put(entityClass, entity);
            }
        } catch (ClassNotFoundException classNotFoundException) {
            logger.fatal("Configuration file TMDMFramework.xml contains wrong class name : "
                    + classNotFoundException.getMessage());
            throw new RuntimeException("Configuration file TMDMFramework.xml contains wrong class name : "
                    + classNotFoundException.getMessage());
        } catch (DMFrameworkException dmFrameworkException) {
            logger.fatal(dmFrameworkException.getMessage());
            throw new RuntimeException(dmFrameworkException.getMessage());
        }
    }

    private EntityManager() {
    }

    public static Entity get(Class entityClass) throws DMFrameworkException {
        Entity entity = entities.get(entityClass);
        if (entity == null) {
            entity = new Entity(entityClass);
            populate(entity);
            entities.put(entityClass, entity);
        }
        return entity;
    }

    public static void populate(Entity entity) throws DMFrameworkException {
        boolean isView = false;
        Class entityClass = entity.getEntityClass();
        Method methods[] = entityClass.getMethods();
        com.thinking.machines.dmframework.annotations.Table tableAnnotation = null;
        com.thinking.machines.dmframework.annotations.View viewAnnotation = null;
        tableAnnotation = (com.thinking.machines.dmframework.annotations.Table) entityClass
                .getAnnotation(com.thinking.machines.dmframework.annotations.Table.class);
        if (tableAnnotation == null) {
            viewAnnotation = (com.thinking.machines.dmframework.annotations.View) entityClass
                    .getAnnotation(com.thinking.machines.dmframework.annotations.View.class);
        }
        if (tableAnnotation == null && viewAnnotation == null) {
            throw new DMFrameworkException(entityClass.getName() + " is not mapped to Table/View");
        }
        if (tableAnnotation == null)
            isView = true;
        String tableName = null;
        String viewName = null;
        if (isView == false) {
            tableName = tableAnnotation.name();
            if (tableName == null || tableName.trim().length() == 0) {
                tableName = entityClass.getSimpleName();
            }
        } else {
            viewName = viewAnnotation.name();
            if (viewName == null || viewName.trim().length() == 0) {
                viewName = entityClass.getSimpleName();
            }
        }
        Table databaseTable = null;
        View databaseView = null;
        if (isView == false) {
            databaseTable = database.getTable(tableName);
            if (databaseTable == null) {
                throw new DMFrameworkException("Table : " + tableName + " does not exist.");
            }
            if (databaseTable.getClassName().equals(entityClass.getSimpleName()) == false) {
                databaseTable.setClassName(entityClass.getSimpleName());
            }
            if (databaseTable.getPrimaryKeys().size() == 0) {
                throw new DMFrameworkException("Primary key missing in table : " + tableName);
            }
        } else {
            databaseView = database.getView(viewName);
            if (databaseView == null) {
                throw new DMFrameworkException("View: " + viewName + " does not exist.");
            }
            if (databaseView.getClassName().equals(entityClass.getSimpleName()) == false) {
                databaseView.setClassName(entityClass.getSimpleName());
            }
        }
        com.thinking.machines.dmframework.annotations.Display displayAnnotation;
        displayAnnotation = (com.thinking.machines.dmframework.annotations.Display) entityClass
                .getAnnotation(com.thinking.machines.dmframework.annotations.Display.class);
        String displayEntityName = "";
        if (displayAnnotation != null) {
            displayEntityName = displayAnnotation.value().trim();
        }
        if (displayEntityName.length() == 0) {
            displayEntityName = entityClass.getSimpleName();
        }
        entity.setDisplayName(displayEntityName);
        if (isView == false) {
            entity.setTable(databaseTable);
            databaseTable.setDisplayName(displayEntityName);
        } else {
            entity.setView(databaseView);
            databaseView.setDisplayName(displayEntityName);
        }
        // Henceforth View will be looked upon as a table with no options to add and no
        // keys
        List<Column> tableColumns;
        if (isView == false) {
            tableColumns = databaseTable.getColumns();
        } else {
            tableColumns = databaseView.getColumns();
        }
        HashMap<Integer, Pair<Field, Column>> fieldColumnHashMap;
        int position;
        Column column = null;
        fieldColumnHashMap = new HashMap<Integer, Pair<Field, Column>>();
        String columnName;
        Field fields[] = entityClass.getDeclaredFields();
        com.thinking.machines.dmframework.annotations.Column columnAnnotation;
        com.thinking.machines.dmframework.annotations.Sequence sequenceAnnotation;
        String displayFieldName;
        String nameOfSequence;
        for (Field field : fields) {
            columnAnnotation = (com.thinking.machines.dmframework.annotations.Column) field
                    .getAnnotation(com.thinking.machines.dmframework.annotations.Column.class);
            if (columnAnnotation != null) {
                columnName = columnAnnotation.name();
                if (columnName == null || columnName.trim().length() == 0) {
                    columnName = field.getName();
                }
                position = 0;
                while (position < tableColumns.size()) {
                    column = tableColumns.get(position);
                    if (columnName.equals(column.getName())) {
                        if (field.getType().equals(column.getProperty().getType()) == false) {
                            throw new DMFrameworkException("Property : " + field.getName() + " should be of : "
                                    + column.getProperty().getType().getName());
                        }
                        break;
                    }
                    position++;
                }
                if (position == tableColumns.size()) {
                    throw new DMFrameworkException("Invalid column name : " + columnName + " in "
                            + entity.getEntityClass().getName() + " against property : " + field.getName());
                }
                if (field.getName().equals(column.getName()) == false) {
                    column.getProperty().setName(field.getName());
                }
                if (database.getArchitecture().equalsIgnoreCase("oracle 11g")) {
                    sequenceAnnotation = (com.thinking.machines.dmframework.annotations.Sequence) field
                            .getAnnotation(com.thinking.machines.dmframework.annotations.Sequence.class);
                    if (sequenceAnnotation != null) {
                        nameOfSequence = sequenceAnnotation.name();
                        if (database.containsSequence(nameOfSequence) == false) {
                            throw new DMFrameworkException(
                                    "No sequence found by the name of  : " + nameOfSequence + " specified against : "
                                            + field.getName() + " in class : " + entityClass.getClass().getName());
                        }
                        column.setIsSequenceAssociated(true);
                        column.setNameOfSequence(nameOfSequence);
                    }
                }
                fieldColumnHashMap.put(new Integer(position + 1), new Pair<Field, Column>(field, column));
                displayAnnotation = (com.thinking.machines.dmframework.annotations.Display) field
                        .getAnnotation(com.thinking.machines.dmframework.annotations.Display.class);
                displayFieldName = null;
                if (displayAnnotation != null) {
                    displayFieldName = displayAnnotation.value().trim();
                }
                if (displayFieldName != null && displayFieldName.length() > 0) {
                    column.setDisplayName(displayFieldName);
                }
            }
        }
        position = 0;
        while (position < tableColumns.size()) {
            if (fieldColumnHashMap.get(new Integer(position + 1)) == null) {
                throw new DMFrameworkException("No property found for column : " + tableColumns.get(position).getName()
                        + " in class : " + entityClass.getClass().getName());
            }
            position++;
        }
        // databaseTable and fieldColumnHashMap are ready
        HashMap<Integer, Pair<MethodWrapper, MethodWrapper>> setterGetterMethodHashMap;
        ArrayList<MethodWrapper> selectWrapperSetterMethods;
        selectWrapperSetterMethods = new ArrayList<MethodWrapper>();
        setterGetterMethodHashMap = new HashMap<Integer, Pair<MethodWrapper, MethodWrapper>>();
        MethodWrapper first;
        MethodWrapper second;
        String methodSuffix;
        Pair<Field, Column> pair;
        String setterName, getterName;
        int k, m;
        int unicode;
        Field field;
        Method method;
        position = 1;
        while (position <= fieldColumnHashMap.size()) {
            pair = fieldColumnHashMap.get(position);
            field = pair.getFirst();
            column = pair.getSecond();
            methodSuffix = field.getName();
            unicode = methodSuffix.charAt(0);
            if (unicode >= 97 && unicode <= 122) {
                if (methodSuffix.length() > 1) {
                    methodSuffix = String.valueOf((char) (unicode - 32)) + methodSuffix.substring(1);
                } else {
                    methodSuffix = String.valueOf((char) (unicode - 32));
                }
            }
            setterName = "set" + methodSuffix;
            Class parameterTypes[];
            k = 0;
            while (k < methods.length) {
                if (methods[k].getName().equals(setterName)) {
                    parameterTypes = methods[k].getParameterTypes();
                    if (parameterTypes.length == 1 && parameterTypes[0].equals(field.getType()))
                        break;
                }
                k++;
            }
            if (k == methods.length) {
                throw new DMFrameworkException(setterName + "(" + field.getType().getName() + ") not found in class : "
                        + entityClass.getName());
            }
            first = new MethodWrapper(methods[k], field.getName(), column);
            getterName = "get" + methodSuffix;
            k = 0;
            while (k < methods.length) {
                if (methods[k].getName().equals(getterName)) {
                    parameterTypes = methods[k].getParameterTypes();
                    if (parameterTypes.length == 0)
                        break;
                }
                k++;
            }
            if (k == methods.length) {
                throw new DMFrameworkException(getterName + "() not found in class : " + entityClass.getName());
            }
            second = new MethodWrapper(methods[k], field.getName(), column);
            setterGetterMethodHashMap.put(new Integer(position), new Pair<MethodWrapper, MethodWrapper>(first, second));
            selectWrapperSetterMethods.add(first);
            position++;
        }
        // databaseTable,fieldColumnHashMap and setterGetterHashMap are ready
        entity.setFieldColumnHashMap(fieldColumnHashMap);
        entity.setSetterGetterMethodHashMap(setterGetterMethodHashMap);
        int i;
        DataType dataType;
        Class preparedStatementClass = java.sql.PreparedStatement.class;
        Class resultSetClass = java.sql.ResultSet.class;
        if (isView == false)
        // validators and keyPortions starts for Table
        {
            ArrayList<MethodWrapper> requiredValidatorGetterMethods;
            requiredValidatorGetterMethods = new ArrayList<MethodWrapper>();
            ArrayList<MethodWrapper> requiredValidatorSetterMethods;
            requiredValidatorSetterMethods = new ArrayList<MethodWrapper>();
            k = 0;
            while (k < tableColumns.size()) {
                column = tableColumns.get(k);
                if (column.getIsNullable() == false) {
                    requiredValidatorGetterMethods.add(setterGetterMethodHashMap.get(k + 1).getSecond());
                    requiredValidatorSetterMethods.add(setterGetterMethodHashMap.get(k + 1).getFirst());
                }
                k++;
            }
            Validator requiredValidator = new RequiredValidator(requiredValidatorSetterMethods,
                    requiredValidatorGetterMethods);
            // databaseTable,fieldColumnHashMap,setterGetterMethodHashMap and
            // requiredValidator are ready
            ArrayList<MethodWrapper> overflowValidatorGetterMethods;
            overflowValidatorGetterMethods = new ArrayList<MethodWrapper>();
            HashSet<Integer> validateForOverflow;
            validateForOverflow = new HashSet<Integer>();
            validateForOverflow.add(java.sql.Types.INTEGER);
            validateForOverflow.add(java.sql.Types.TINYINT);
            validateForOverflow.add(java.sql.Types.SMALLINT);
            validateForOverflow.add(java.sql.Types.BIGINT);
            validateForOverflow.add(java.sql.Types.DECIMAL);
            validateForOverflow.add(java.sql.Types.NUMERIC);
            validateForOverflow.add(java.sql.Types.CHAR);
            validateForOverflow.add(java.sql.Types.LONGVARCHAR);
            validateForOverflow.add(java.sql.Types.NCHAR);
            validateForOverflow.add(java.sql.Types.NVARCHAR);
            k = 0;
            while (k < tableColumns.size()) {
                column = tableColumns.get(k);
                if (validateForOverflow.contains(column.getType())) {
                    overflowValidatorGetterMethods.add(setterGetterMethodHashMap.get(k + 1).getSecond());
                }
                k++;
            }
            Validator overflowValidator = new OverflowValidator(overflowValidatorGetterMethods);
            /*
             * databaseTable
             * fieldColumnHashMap
             * setterGetterMethodHashMap
             * requiredValidator
             * and
             * overFlowValidator
             * are ready
             */
            ArrayList<Method> primaryKeyPreparedStatementSetterMethods;
            primaryKeyPreparedStatementSetterMethods = new ArrayList<Method>();
            ArrayList<MethodWrapper> primaryKeyValidatorGetterMethods;
            primaryKeyValidatorGetterMethods = new ArrayList<MethodWrapper>();
            List<String> primaryKeys = databaseTable.getPrimaryKeys();
            String primaryKeyColumnName;
            String primaryKeyExceptionMessage = "";
            boolean applyAnd = false;
            Method preparedStatementSetterMethod;
            for (i = 0; i < primaryKeys.size(); i++) {
                primaryKeyColumnName = primaryKeys.get(i);
                k = 0;
                while (k < tableColumns.size()) {
                    column = tableColumns.get(k);
                    if (primaryKeyColumnName.equals(column.getName())) {
                        dataType = Types.getDataType(column.getType());
                        try {
                            preparedStatementSetterMethod = preparedStatementClass.getMethod(
                                    "set" + dataType.getSetterGetterMethodName(), int.class,
                                    dataType.getPrimitiveType());
                        } catch (Exception ex) {
                            preparedStatementSetterMethod = null;
                            // no exception can arise over here as everything is 100% known
                        }
                        primaryKeyPreparedStatementSetterMethods.add(preparedStatementSetterMethod);
                        primaryKeyValidatorGetterMethods.add(setterGetterMethodHashMap.get(k + 1).getSecond());
                        if (applyAnd) {
                            primaryKeyExceptionMessage = primaryKeyExceptionMessage + " and ";
                            primaryKeyExceptionMessage = primaryKeyExceptionMessage
                                    + setterGetterMethodHashMap.get(new Integer(k + 1)).getSecond().getSpacedProperty();
                        } else {
                            primaryKeyExceptionMessage = setterGetterMethodHashMap.get(new Integer(k + 1)).getSecond()
                                    .getCapitalizedSpacedProperty();
                        }
                        applyAnd = true;
                        break;
                    }
                    k++;
                }
            }
            KeyValidator primaryKeyValidator = new PrimaryKeyValidator(databaseTable.getPrimaryKeyExistsSQL(),
                    primaryKeyValidatorGetterMethods, primaryKeyPreparedStatementSetterMethods,
                    primaryKeyExceptionMessage);
            /*
             * databaseTable
             * fieldColumnHashMap
             * setterGetterMethodHashMap
             * requiredValidator
             * overFlowValidator
             * and
             * primaryKeyValidator
             * are ready
             */
            // Unique part starts
            int mi;
            ArrayList<Method> uniqueKeyPreparedStatementSetterMethods;
            ArrayList<MethodWrapper> uniqueKeyValidatorGetterMethods;
            HashMap<String, LinkedList<Column>> uniqueKeys = databaseTable.getUniqueKeys();
            Set<String> uniqueKeyIndexNames = databaseTable.getUniqueKeysIndexNames();
            String uniqueKeyExceptionMessage = "";
            String uniqueKeyExistsSQL;
            ArrayList<UniqueKeyWrapper> uniqueKeyWrappers;
            uniqueKeyWrappers = new ArrayList<UniqueKeyWrapper>();
            UniqueKeyWrapper uniqueKeyWrapper;
            LinkedList<Column> uniqueKeyColumnsGroup;
            for (String uniqueKeyIndexName : uniqueKeyIndexNames) {
                uniqueKeyColumnsGroup = uniqueKeys.get(uniqueKeyIndexName);
                uniqueKeyPreparedStatementSetterMethods = new ArrayList<Method>();
                uniqueKeyValidatorGetterMethods = new ArrayList<MethodWrapper>();
                uniqueKeyExistsSQL = databaseTable.getUniqueKeyExistsSQL(uniqueKeyIndexName);
                uniqueKeyExceptionMessage = "";
                applyAnd = false;
                k = 0;
                while (k < uniqueKeyColumnsGroup.size()) {
                    column = uniqueKeyColumnsGroup.get(k);
                    dataType = Types.getDataType(column.getType());
                    try {
                        preparedStatementSetterMethod = preparedStatementClass.getMethod(
                                "set" + dataType.getSetterGetterMethodName(), int.class, dataType.getPrimitiveType());
                    } catch (Exception ex) {
                        preparedStatementSetterMethod = null;
                        // no exception can arise over here as everything is 100% known
                    }
                    uniqueKeyPreparedStatementSetterMethods.add(preparedStatementSetterMethod);
                    for (mi = 0; mi < setterGetterMethodHashMap.size(); mi++) {
                        if (setterGetterMethodHashMap.get(new Integer(mi + 1)).getFirst().getColumn().getName()
                                .equals(column.getName()))
                            break;
                    }
                    uniqueKeyValidatorGetterMethods.add(setterGetterMethodHashMap.get(mi + 1).getSecond());
                    if (applyAnd) {
                        uniqueKeyExceptionMessage = uniqueKeyExceptionMessage + " and ";
                        uniqueKeyExceptionMessage = uniqueKeyExceptionMessage
                                + setterGetterMethodHashMap.get(new Integer(mi + 1)).getSecond().getSpacedProperty();
                    } else {
                        uniqueKeyExceptionMessage = uniqueKeyExceptionMessage + setterGetterMethodHashMap
                                .get(new Integer(mi + 1)).getSecond().getCapitalizedSpacedProperty();
                    }
                    applyAnd = true;
                    k++;
                }
                uniqueKeyWrapper = new UniqueKeyWrapper(uniqueKeyValidatorGetterMethods,
                        uniqueKeyPreparedStatementSetterMethods, uniqueKeyExceptionMessage, uniqueKeyExistsSQL);
                uniqueKeyWrappers.add(uniqueKeyWrapper);
            }
            KeyValidator uniqueKeyValidator = new UniqueKeyValidator(uniqueKeyWrappers);
            /*
             * databaseTable
             * fieldColumnHashMap
             * setterGetterMethodHashMap
             * requiredValidator
             * overFlowValidator
             * primaryKeyValidator
             * and
             * uniqueKeyValidator
             * are ready
             */
            // Unique Key for update part starts
            ArrayList<Method> uniqueKeyForUpdateOperationPreparedStatementSetterMethods;
            ArrayList<MethodWrapper> uniqueKeyForUpdateOperationValidatorGetterMethods;
            String uniqueKeyForUpdateOperationColumnName;
            String uniqueKeyForUpdateOperationExceptionMessage = "";
            String uniqueKeyForUpdateOperationExistsSQL;
            ArrayList<UniqueKeyWrapper> uniqueKeyForUpdateOperationWrappers;
            uniqueKeyForUpdateOperationWrappers = new ArrayList<UniqueKeyWrapper>();
            UniqueKeyWrapper uniqueKeyForUpdateOperationWrapper;
            for (String uniqueKeyIndexName : uniqueKeyIndexNames) {
                uniqueKeyColumnsGroup = uniqueKeys.get(uniqueKeyIndexName);
                uniqueKeyForUpdateOperationPreparedStatementSetterMethods = new ArrayList<Method>();
                uniqueKeyForUpdateOperationValidatorGetterMethods = new ArrayList<MethodWrapper>();
                uniqueKeyForUpdateOperationExistsSQL = databaseTable
                        .getUniqueKeyExistsSQLForUpdateOperation(uniqueKeyIndexName);
                uniqueKeyForUpdateOperationExceptionMessage = "";
                applyAnd = false;
                k = 0;
                while (k < uniqueKeyColumnsGroup.size()) {
                    column = uniqueKeyColumnsGroup.get(k);
                    dataType = Types.getDataType(column.getType());
                    try {
                        preparedStatementSetterMethod = preparedStatementClass.getMethod(
                                "set" + dataType.getSetterGetterMethodName(), int.class, dataType.getPrimitiveType());
                    } catch (Exception ex) {
                        preparedStatementSetterMethod = null;
                        // no exception can arise over here as everything is 100% known
                    }
                    uniqueKeyForUpdateOperationPreparedStatementSetterMethods.add(preparedStatementSetterMethod);
                    for (mi = 0; mi < setterGetterMethodHashMap.size(); mi++) {
                        if (setterGetterMethodHashMap.get(new Integer(mi + 1)).getFirst().getColumn().getName()
                                .equals(column.getName()))
                            break;
                    }
                    uniqueKeyForUpdateOperationValidatorGetterMethods
                            .add(setterGetterMethodHashMap.get(mi + 1).getSecond());
                    if (applyAnd) {
                        uniqueKeyForUpdateOperationExceptionMessage = uniqueKeyForUpdateOperationExceptionMessage
                                + " and ";
                        uniqueKeyForUpdateOperationExceptionMessage = uniqueKeyForUpdateOperationExceptionMessage
                                + setterGetterMethodHashMap.get(new Integer(mi + 1)).getSecond().getSpacedProperty();
                    } else {
                        uniqueKeyForUpdateOperationExceptionMessage = uniqueKeyForUpdateOperationExceptionMessage
                                + setterGetterMethodHashMap.get(new Integer(mi + 1)).getSecond()
                                        .getCapitalizedSpacedProperty();
                    }
                    applyAnd = true;
                    k++;
                }
                // add primary keys setters in existsForUpdateOperation SQL Statement (the or
                // factor)
                for (i = 0; i < primaryKeys.size(); i++) {
                    primaryKeyColumnName = primaryKeys.get(i);
                    k = 0;
                    while (k < tableColumns.size()) {
                        column = tableColumns.get(k);
                        if (primaryKeyColumnName.equals(column.getName())) {
                            dataType = Types.getDataType(column.getType());
                            try {
                                preparedStatementSetterMethod = preparedStatementClass.getMethod(
                                        "set" + dataType.getSetterGetterMethodName(), int.class,
                                        dataType.getPrimitiveType());
                            } catch (Exception ex) {
                                preparedStatementSetterMethod = null;
                                // no exception can arise over here as everything is 100% known
                            }
                            uniqueKeyForUpdateOperationPreparedStatementSetterMethods
                                    .add(preparedStatementSetterMethod);
                            uniqueKeyForUpdateOperationValidatorGetterMethods
                                    .add(setterGetterMethodHashMap.get(k + 1).getSecond());
                            break;
                        }
                        k++;
                    }
                }
                uniqueKeyForUpdateOperationWrapper = new UniqueKeyWrapper(
                        uniqueKeyForUpdateOperationValidatorGetterMethods,
                        uniqueKeyForUpdateOperationPreparedStatementSetterMethods,
                        uniqueKeyForUpdateOperationExceptionMessage, uniqueKeyForUpdateOperationExistsSQL, true,
                        primaryKeys.size());
                uniqueKeyForUpdateOperationWrappers.add(uniqueKeyForUpdateOperationWrapper);
            }
            KeyValidator uniqueKeyValidatorForUpdateOperation = new UniqueKeyValidatorForUpdateOperation(
                    uniqueKeyForUpdateOperationWrappers);
            /*
             * databaseTable
             * fieldColumnHashMap
             * setterGetterMethodHashMap
             * requiredValidator
             * overFlowValidator
             * primaryKeyValidator
             * uniqueKeyValidator
             * and
             * uniqueKeyValidatorForUpdateOperation
             * are ready
             */
            ArrayList<ParentWrapper> parentWrappers;
            parentWrappers = new ArrayList<ParentWrapper>();
            ArrayList<MethodWrapper> childGetterMethods;
            ArrayList<Method> parentPreparedStatementSetterMethods;
            ParentWrapper parentWrapper;
            String parentExceptionMessage;
            String parentExistsSQL;
            Table parentTable;
            Column childColumn;
            Column parentColumn;
            ForeignKey foreignKey;
            ArrayList<String> parentTables;
            parentTables = databaseTable.getParentTables();
            HashMap<Integer, Pair<Column, Column>> foreignKeyColumnMappings;
            Pair<Column, Column> foreignKeyPair;
            int j;
            for (i = 0; i < parentTables.size(); i++) {
                childGetterMethods = new ArrayList<MethodWrapper>();
                parentPreparedStatementSetterMethods = new ArrayList<Method>();
                parentExceptionMessage = "";
                parentTable = database.getTable(parentTables.get(i));
                foreignKey = databaseTable.getForeignKey(parentTable.getName());
                parentExistsSQL = foreignKey.getExistsSQL();
                foreignKeyColumnMappings = foreignKey.getColumnMappings();
                applyAnd = false;
                for (k = 1; k <= foreignKeyColumnMappings.size(); k++) {
                    foreignKeyPair = foreignKeyColumnMappings.get(k);
                    childColumn = foreignKeyPair.getFirst();
                    parentColumn = foreignKeyPair.getSecond();
                    dataType = Types.getDataType(parentColumn.getType());
                    try {
                        preparedStatementSetterMethod = preparedStatementClass.getMethod(
                                "set" + dataType.getSetterGetterMethodName(), int.class, dataType.getPrimitiveType());
                    } catch (Exception ex) {
                        preparedStatementSetterMethod = null;
                        // no exception can arise over here as everything is 100% known
                    }
                    parentPreparedStatementSetterMethods.add(preparedStatementSetterMethod);
                    for (m = 0; m < tableColumns.size(); m++) {
                        if (childColumn.getName().equals(tableColumns.get(m).getName()))
                            break;
                    }
                    childGetterMethods.add(setterGetterMethodHashMap.get(m + 1).getSecond());
                    if (applyAnd) {
                        parentExceptionMessage = parentExceptionMessage + " and ";
                        parentExceptionMessage = parentExceptionMessage
                                + setterGetterMethodHashMap.get(new Integer(m + 1)).getSecond().getSpacedProperty();
                    } else {
                        parentExceptionMessage = setterGetterMethodHashMap.get(new Integer(m + 1)).getSecond()
                                .getCapitalizedSpacedProperty();
                    }
                    applyAnd = true;
                }
                parentWrapper = new ParentWrapper(childGetterMethods, parentPreparedStatementSetterMethods,
                        parentExceptionMessage, parentExistsSQL, databaseTable, parentTable);
                parentWrappers.add(parentWrapper);
            }
            KeyValidator parentValidator = new ParentValidator(parentWrappers);
            /*
             * databaseTable
             * fieldColumnHashMap
             * setterGetterMethodHashMap
             * requiredValidator
             * overFlowValidator
             * primaryKeyValidator
             * uniqueKeyValidator
             * uniqueKeyValidatorForUpdateOperation
             * and
             * parentValidator
             * are ready
             */
            ArrayList<ChildWrapper> childWrappers;
            childWrappers = new ArrayList<ChildWrapper>();
            ArrayList<MethodWrapper> parentGetterMethods;
            ArrayList<Method> childPreparedStatementSetterMethods;
            ChildWrapper childWrapper;
            String childExceptionMessage;
            String childExistsSQL;
            Table childTable;
            ExportedKey exportedKey;
            ArrayList<String> childTables;
            childTables = databaseTable.getChildTables();
            HashMap<Integer, Pair<Column, Column>> exportedKeyColumnMappings;
            Pair<Column, Column> exportedKeyPair;
            for (i = 0; i < childTables.size(); i++) {
                parentGetterMethods = new ArrayList<MethodWrapper>();
                childPreparedStatementSetterMethods = new ArrayList<Method>();
                childExceptionMessage = "";
                childTable = database.getTable(childTables.get(i));
                exportedKey = databaseTable.getExportedKey(childTable.getName());
                childExistsSQL = exportedKey.getExistsSQL();
                exportedKeyColumnMappings = exportedKey.getColumnMappings();
                applyAnd = false;
                for (k = 1; k <= exportedKeyColumnMappings.size(); k++) {
                    exportedKeyPair = exportedKeyColumnMappings.get(k);
                    parentColumn = exportedKeyPair.getFirst();
                    childColumn = exportedKeyPair.getSecond();
                    dataType = Types.getDataType(childColumn.getType());
                    try {
                        preparedStatementSetterMethod = preparedStatementClass.getMethod(
                                "set" + dataType.getSetterGetterMethodName(), int.class, dataType.getPrimitiveType());
                    } catch (Exception ex) {
                        preparedStatementSetterMethod = null;
                        // no exception can arise over here as everything is 100% known
                    }
                    childPreparedStatementSetterMethods.add(preparedStatementSetterMethod);
                    for (m = 0; m < tableColumns.size(); m++) {
                        if (parentColumn.getName().equals(tableColumns.get(m).getName()))
                            break;
                    }
                    parentGetterMethods.add(setterGetterMethodHashMap.get(m + 1).getSecond());
                    if (applyAnd) {
                        childExceptionMessage = childExceptionMessage + " and ";
                        childExceptionMessage = childExceptionMessage
                                + setterGetterMethodHashMap.get(new Integer(m + 1)).getSecond().getSpacedProperty();
                    } else {
                        childExceptionMessage = setterGetterMethodHashMap.get(new Integer(m + 1)).getSecond()
                                .getCapitalizedSpacedProperty();
                    }
                    applyAnd = true;
                }
                childWrapper = new ChildWrapper(parentGetterMethods, childPreparedStatementSetterMethods,
                        childExceptionMessage, childExistsSQL, childTable, databaseTable);
                childWrappers.add(childWrapper);
            }
            KeyValidator childValidator = new ChildValidator(childWrappers);
            /*
             * databaseTable
             * fieldColumnHashMap
             * setterGetterMethodHashMap
             * requiredValidator
             * overFlowValidator
             * primaryKeyValidator
             * uniqueKeyValidator
             * uniqueKeyValidatorForUpdateOperation
             * parentValidator
             * and
             * childValidator
             * are ready
             */
            InsertWrapper insertWrapper;
            try {
                String nextValSQLStatement;
                MethodWrapper nextValObjectPropertyGetter;
                MethodWrapper nextValObjectPropertySetter;
                Method nextValResultSetGetter;
                HashMap<Integer, SequenceWrapper> sequenceWrappers = null;
                SequenceWrapper sequenceWrapper;
                String insertWrapperSQLStatement = databaseTable.getInsertSQL();
                ArrayList<MethodWrapper> insertWrapperGetterMethods;
                ArrayList<Method> insertWrapperPreparedStatementSetterMethods;
                MethodWrapper insertWrapperAutoIncrementedPropertySetter = null;
                Method insertWrapperAutoIncrementedPropertyResultSetGetter = null;
                insertWrapperPreparedStatementSetterMethods = new ArrayList<Method>();
                insertWrapperGetterMethods = new ArrayList<MethodWrapper>();
                tableColumns = databaseTable.getColumns();
                for (i = 0; i < tableColumns.size(); i++) {
                    column = tableColumns.get(i);
                    if (column.getIsSequenceAssociated()) {
                        nextValSQLStatement = "select " + column.getNameOfSequence()
                                + ".nextVal as nextValue from dual";
                        nextValObjectPropertySetter = setterGetterMethodHashMap.get(i + 1).getFirst();
                        nextValObjectPropertyGetter = setterGetterMethodHashMap.get(i + 1).getSecond();
                        dataType = Types.getDataType(column.getType());
                        try {
                            nextValResultSetGetter = resultSetClass
                                    .getMethod("get" + dataType.getSetterGetterMethodName(), int.class);
                        } catch (Exception ex) {
                            nextValResultSetGetter = null;
                            // no exception can arise over here as everything is 100% known
                        }
                        sequenceWrapper = new SequenceWrapper(nextValSQLStatement, nextValObjectPropertySetter,
                                nextValObjectPropertyGetter, nextValResultSetGetter);
                        if (sequenceWrappers == null) {
                            sequenceWrappers = new HashMap<Integer, SequenceWrapper>();
                        }
                        sequenceWrappers.put(new Integer(i + 1), sequenceWrapper);
                    }
                    if (column.getIsAutoIncrementEnabled()) {
                        dataType = Types.getDataType(column.getType());
                        try {
                            insertWrapperAutoIncrementedPropertyResultSetGetter = resultSetClass
                                    .getMethod("get" + dataType.getSetterGetterMethodName(), int.class);
                            insertWrapperAutoIncrementedPropertySetter = setterGetterMethodHashMap
                                    .get(new Integer(i + 1)).getFirst();
                        } catch (Exception ex) {
                            insertWrapperAutoIncrementedPropertyResultSetGetter = null;
                            // no exception can arise over here as everything is 100% known
                        }
                    } else {
                        dataType = Types.getDataType(column.getType());
                        try {
                            preparedStatementSetterMethod = preparedStatementClass.getMethod(
                                    "set" + dataType.getSetterGetterMethodName(), int.class,
                                    dataType.getPrimitiveType());
                        } catch (Exception ex) {
                            preparedStatementSetterMethod = null;
                            // no exception can arise over here as everything is 100% known
                        }
                        insertWrapperPreparedStatementSetterMethods.add(preparedStatementSetterMethod);
                        insertWrapperGetterMethods.add(setterGetterMethodHashMap.get(i + 1).getSecond());
                    }
                }
                insertWrapper = new InsertWrapper(insertWrapperSQLStatement, insertWrapperGetterMethods,
                        insertWrapperPreparedStatementSetterMethods, insertWrapperAutoIncrementedPropertySetter,
                        insertWrapperAutoIncrementedPropertyResultSetGetter, sequenceWrappers);
            } catch (DMFrameworkException dmFrameworkException) {
                insertWrapper = null;
            }
            /*
             * databaseTable
             * fieldColumnHashMap
             * setterGetterMethodHashMap
             * requiredValidator
             * overFlowValidator
             * primaryKeyValidator
             * uniqueKeyValidator
             * uniqueKeyValidatorForUpdateOperation
             * parentValidator
             * childValidator
             * and
             * insertWrapper
             * are ready
             */
            UpdateWrapper updateWrapper;
            try {
                String updateWrapperSQLStatement = databaseTable.getUpdateSQL();
                ArrayList<MethodWrapper> updateWrapperGetterMethods;
                ArrayList<Method> updateWrapperPreparedStatementSetterMethods;
                updateWrapperPreparedStatementSetterMethods = new ArrayList<Method>();
                updateWrapperGetterMethods = new ArrayList<MethodWrapper>();
                tableColumns = databaseTable.getColumns();
                boolean isPrimaryKey;
                for (i = 0; i < tableColumns.size(); i++) {
                    column = tableColumns.get(i);
                    isPrimaryKey = false;
                    for (k = 0; k < primaryKeys.size(); k++) {
                        if (primaryKeys.get(k).equals(column.getName())) {
                            isPrimaryKey = true;
                            break;
                        }
                    }
                    if (isPrimaryKey)
                        continue;
                    dataType = Types.getDataType(column.getType());
                    try {
                        preparedStatementSetterMethod = preparedStatementClass.getMethod(
                                "set" + dataType.getSetterGetterMethodName(), int.class, dataType.getPrimitiveType());
                    } catch (Exception ex) {
                        preparedStatementSetterMethod = null;
                        // no exception can arise over here as everything is 100% known
                    }
                    updateWrapperPreparedStatementSetterMethods.add(preparedStatementSetterMethod);
                    updateWrapperGetterMethods.add(setterGetterMethodHashMap.get(i + 1).getSecond());
                }
                for (i = 0; i < primaryKeys.size(); i++) {
                    for (k = 0; k < tableColumns.size(); k++) {
                        column = tableColumns.get(k);
                        if (primaryKeys.get(i).equals(column.getName())) {
                            dataType = Types.getDataType(column.getType());
                            try {
                                preparedStatementSetterMethod = preparedStatementClass.getMethod(
                                        "set" + dataType.getSetterGetterMethodName(), int.class,
                                        dataType.getPrimitiveType());
                            } catch (Exception ex) {
                                preparedStatementSetterMethod = null;
                                // no exception can arise over here as everything is 100% known
                            }
                            updateWrapperPreparedStatementSetterMethods.add(preparedStatementSetterMethod);
                            updateWrapperGetterMethods.add(setterGetterMethodHashMap.get(k + 1).getSecond());
                            break;
                        }
                    }
                }
                updateWrapper = new UpdateWrapper(updateWrapperSQLStatement, updateWrapperGetterMethods,
                        updateWrapperPreparedStatementSetterMethods);
            } catch (DMFrameworkException dmFrameworkException) {
                updateWrapper = null;
            }
            /*
             * databaseTable
             * fieldColumnHashMap
             * setterGetterMethodHashMap
             * requiredValidator
             * overFlowValidator
             * primaryKeyValidator
             * uniqueKeyValidator
             * uniqueKeyValidatorForUpdateOperation
             * parentValidator
             * childValidator
             * insertWrapper
             * and
             * updateWrapper
             * are ready
             */
            DeleteWrapper deleteWrapper;
            try {
                String deleteWrapperSQLStatement = databaseTable.getDeleteSQL();
                ArrayList<MethodWrapper> deleteWrapperGetterMethods;
                ArrayList<Method> deleteWrapperPreparedStatementSetterMethods;
                deleteWrapperPreparedStatementSetterMethods = new ArrayList<Method>();
                deleteWrapperGetterMethods = new ArrayList<MethodWrapper>();
                tableColumns = databaseTable.getColumns();
                boolean isPrimaryKey;
                for (i = 0; i < primaryKeys.size(); i++) {
                    for (k = 0; k < tableColumns.size(); k++) {
                        column = tableColumns.get(k);
                        if (primaryKeys.get(i).equals(column.getName())) {
                            dataType = Types.getDataType(column.getType());
                            try {
                                preparedStatementSetterMethod = preparedStatementClass.getMethod(
                                        "set" + dataType.getSetterGetterMethodName(), int.class,
                                        dataType.getPrimitiveType());
                            } catch (Exception ex) {
                                preparedStatementSetterMethod = null;
                                // no exception can arise over here as everything is 100% known
                            }
                            deleteWrapperPreparedStatementSetterMethods.add(preparedStatementSetterMethod);
                            deleteWrapperGetterMethods.add(setterGetterMethodHashMap.get(k + 1).getSecond());
                            break;
                        }
                    }
                }
                deleteWrapper = new DeleteWrapper(deleteWrapperSQLStatement, deleteWrapperGetterMethods,
                        deleteWrapperPreparedStatementSetterMethods);
            } catch (DMFrameworkException dmFrameworkException) {
                deleteWrapper = null;
            }
            /*
             * databaseTable
             * fieldColumnHashMap
             * setterGetterMethodHashMap
             * requiredValidator
             * overFlowValidator
             * primaryKeyValidator
             * uniqueKeyValidator
             * uniqueKeyValidatorForUpdateOperation
             * parentValidator
             * childValidator
             * insertWrapper
             * updateWrapper
             * end
             * deleteWrapper
             * are ready
             */
            entity.setRequiredValidator(requiredValidator);
            entity.setOverflowValidator(overflowValidator);
            entity.setPrimaryKeyValidator(primaryKeyValidator);
            entity.setUniqueKeyValidator(uniqueKeyValidator);
            entity.setUniqueKeyValidatorForUpdateOperation(uniqueKeyValidatorForUpdateOperation);
            entity.setParentValidator(parentValidator);
            entity.setChildValidator(childValidator);
            entity.setInsertWrapper(insertWrapper);
            entity.setUpdateWrapper(updateWrapper);
            entity.setDeleteWrapper(deleteWrapper);
        } // validators and keyPortions ends for Table
          // Portion common for Table/View begins
        com.thinking.machines.dmframework.annotations.Sort sortAnnotation;
        TreeMap<Integer, String> defaultSortOrderTreeMap = new TreeMap<Integer, String>();
        boolean descending;
        int priority;
        for (k = 1; k <= fieldColumnHashMap.size(); k++) {
            pair = fieldColumnHashMap.get(new Integer(k));
            field = pair.getFirst();
            sortAnnotation = (com.thinking.machines.dmframework.annotations.Sort) field
                    .getAnnotation(com.thinking.machines.dmframework.annotations.Sort.class);
            if (sortAnnotation != null) {
                column = pair.getSecond();
                priority = sortAnnotation.priority();
                descending = sortAnnotation.descending();
                if (defaultSortOrderTreeMap.get(priority) != null) {
                    throw new DMFrameworkException("Multiple properties of : " + entityClass.getName()
                            + " cannot have same priority level : " + priority);
                }
                if (descending) {
                    defaultSortOrderTreeMap.put(priority, column.getName() + " desc");
                } else {
                    defaultSortOrderTreeMap.put(priority, column.getName());
                }
            }
        }
        String defaultOrderBy = null;
        if (defaultSortOrderTreeMap.size() > 0) {
            StringBuilder defaultOrderByStringBuilder = new StringBuilder();
            defaultOrderByStringBuilder.append(" order by ");
            Iterator<Map.Entry<Integer, String>> defaultSortOrderTreeMapIterator;
            Map.Entry<Integer, String> defaultSortOrderPair;
            defaultSortOrderTreeMapIterator = defaultSortOrderTreeMap.entrySet().iterator();
            boolean applyComma = false;
            while (defaultSortOrderTreeMapIterator.hasNext()) {
                if (applyComma) {
                    defaultOrderByStringBuilder.append(",");
                }
                defaultSortOrderPair = defaultSortOrderTreeMapIterator.next();
                defaultOrderByStringBuilder.append(defaultSortOrderPair.getValue());
                applyComma = true;
            }
            defaultOrderBy = defaultOrderByStringBuilder.toString();
        }
        String selectWrapperSQLStatement;
        ArrayList<Method> selectWrapperSQLGetterMethods;
        ArrayList<Method> selectWrapperPreparedStatementSetterMethods;
        if (isView == false) {
            selectWrapperSQLStatement = databaseTable.getSelectSQL();
        } else {
            selectWrapperSQLStatement = databaseView.getSelectSQL();
        }
        selectWrapperSQLGetterMethods = new ArrayList<Method>();
        selectWrapperPreparedStatementSetterMethods = new ArrayList<Method>();
        MethodWrapper selectWrapperMethodWrapper;
        Method selectWrapperSQLGetterMethod;
        Method selectWrapperPreparedStatementSetterMethod;
        for (i = 0; i < selectWrapperSetterMethods.size(); i++) {
            selectWrapperMethodWrapper = selectWrapperSetterMethods.get(i);
            dataType = Types.getDataType(selectWrapperMethodWrapper.getColumn().getType());
            try {
                selectWrapperSQLGetterMethod = resultSetClass.getMethod("get" + dataType.getSetterGetterMethodName(),
                        String.class);
                selectWrapperPreparedStatementSetterMethod = preparedStatementClass.getMethod(
                        "set" + dataType.getSetterGetterMethodName(), int.class, dataType.getPrimitiveType());
            } catch (Exception ex) {
                selectWrapperSQLGetterMethod = null;
                selectWrapperPreparedStatementSetterMethod = null;
                // no exception can arise over here as everything is 100% known
            }
            selectWrapperSQLGetterMethods.add(selectWrapperSQLGetterMethod);
            selectWrapperPreparedStatementSetterMethods.add(selectWrapperPreparedStatementSetterMethod);
        }
        SelectWrapper selectWrapper = new SelectWrapper(selectWrapperSQLStatement, selectWrapperSetterMethods,
                selectWrapperSQLGetterMethods, selectWrapperPreparedStatementSetterMethods, defaultOrderBy);
        /*
         * selectWrapper is ready
         */
        entity.setSelectWrapper(selectWrapper);
    }
}