package com.thinking.machines.dmframework.pojo;

import java.util.*;

import javax.swing.text.Utilities;

import com.thinking.machines.dmframework.exceptions.*;
import com.thinking.machines.dmframework.utilities.*;

public class Table implements java.io.Serializable, Comparable<Table> {
    private String insertSQL;
    private String updateSQL;
    private String deleteSQL;
    private String selectSQL;
    private String primaryKeyExistsSQL;
    private HashMap<String, String> uniqueKeyExistsSQL;
    private HashMap<String, String> uniqueKeyExistsSQLForUpdateOperation;
    private String name;
    private List<Column> columns;
    private String className;
    private String displayName;
    private List<String> primaryKeys;
    private HashMap<String, ForeignKey> foreignKeys; // String : PK Table name
    private HashMap<String, ExportedKey> exportedKeys; // String : FK Table name
    private HashMap<String, LinkedList<Column>> uniqueKeys; // String : Index Name
    private Database database;

    public Table() {
        this.name = "";
        this.columns = new ArrayList<Column>();
        this.className = "";
        this.primaryKeys = new ArrayList<String>();
        this.foreignKeys = new HashMap<String, ForeignKey>();
        this.exportedKeys = new HashMap<String, ExportedKey>();
        this.displayName = "";
    }

    public void setName(String name) {
        this.name = name;
        setClassName(Utilities.getJavaClassName(this.name));
    }

    public String getName() {
        return this.name;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return this.displayName;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public Column getColumn(String name) {
        for (Column column : columns) {
            if (column.getName().equalsIgnoreCase(name))
                return column;
        }
        return null;
    }

    public List<Column> getColumns() {
        return this.columns;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getClassName() {
        return this.className;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public List<String> getPrimaryKeys() {
        return this.primaryKeys;
    }

    public void setForeignKeys(HashMap<String, ForeignKey> foreignKeys) {
        this.foreignKeys = foreignKeys;
    }

    public HashMap<String, ForeignKey> getForeignKeys() {
        return this.foreignKeys;
    }

    public ForeignKey getForeignKey(String mappedToTableName) {
        return this.foreignKeys.get(mappedToTableName);
    }

    public ExportedKey getExportedKey(String mappedToTableName) {
        return this.exportedKeys.get(mappedToTableName);
    }

    public void setExportedKeys(HashMap<String, ExportedKey> exportedKeys) {
        this.exportedKeys = exportedKeys;
    }

    public HashMap<String, ExportedKey> getExportedKeys() {
        return this.exportedKeys;
    }

    public boolean equals(Object object) {
        if (!(object instanceof Table))
            return false;
        Table another = (Table) object;
        return this.name.equals(another.name);
    }

    public int compareTo(Table table) {
        return this.name.compareTo(table.name);
    }

    public int hashCode() {
        return this.name.hashCode();
    }

    public void addColumn(Column column) {
        columns.add(column);
    }

    public void addPrimaryKey(String primaryKey) {
        this.primaryKeys.add(primaryKey);
    }

    public Column getPrimaryKeyColumn(String primaryKey) {
        for (Column column : columns) {
            if (column.getName().equals(primaryKey))
                return column;
        }
        return null;
    }

    public void addForeignKey(String mappedToTable, int sequenceNumber, Pair<Column, Column> columnMapping) {
        ForeignKey foreignKey;
        foreignKey = foreignKeys.get(mappedToTable);
        if (foreignKey == null) {
            foreignKey = new ForeignKey();
            foreignKey.setMappedToTable(mappedToTable);
            foreignKeys.put(mappedToTable, foreignKey);
        }
        foreignKey.addColumnMapping(sequenceNumber, columnMapping);
    }

    public void addExportedKey(String mappedToTable, int sequenceNumber, Pair<Column, Column> columnMapping) {
        ExportedKey exportedKey;
        exportedKey = exportedKeys.get(mappedToTable);
        if (exportedKey == null) {
            exportedKey = new ExportedKey();
            exportedKey.setMappedToTable(mappedToTable);
            exportedKeys.put(mappedToTable, exportedKey);
        }
        exportedKey.addColumnMapping(sequenceNumber, columnMapping);
    }

    public void buildSQL(boolean buildInsertSQL, boolean buildUpdateSQL, boolean buildDeleteSQL) {
        StringBuilder sb;
        boolean applyComma;
        Column column;
        boolean applyAnd;
        selectSQL = "select * from " + this.getName();
        if (buildInsertSQL) {
            sb = new StringBuilder();
            sb.append("insert into ");
            sb.append(this.getName());
            sb.append("(");
            applyComma = false;
            for (int i = 0; i < columns.size(); i++) {
                column = columns.get(i);
                if (column.getIsAutoIncrementEnabled() == false) {
                    if (applyComma)
                        sb.append(",");
                    sb.append(column.getName());
                    applyComma = true;
                }
            }
            sb.append(") values(");
            applyComma = false;
            for (int i = 0; i < columns.size(); i++) {
                column = columns.get(i);
                if (column.getIsAutoIncrementEnabled() == false) {
                    if (applyComma)
                        sb.append(",");
                    sb.append("?");
                    applyComma = true;
                }
            }
            sb.append(")");
            this.insertSQL = sb.toString();
        }
        if (buildUpdateSQL) {
            sb = new StringBuilder();
            sb.append("update ");
            sb.append(this.getName());
            sb.append(" set ");
            applyComma = false;
            boolean isPrimaryKey;
            for (int i = 0; i < columns.size(); i++) {
                column = columns.get(i);
                isPrimaryKey = false;
                for (int k = 0; k < primaryKeys.size(); k++) {
                    if (primaryKeys.get(k).equals(column.getName())) {
                        isPrimaryKey = true;
                        break;
                    }
                }
                if (isPrimaryKey)
                    continue;
                if (applyComma)
                    sb.append(",");
                sb.append(column.getName());
                sb.append("=?");
                applyComma = true;
            }
            if (applyComma) {
                sb.append(" where ");
                applyAnd = false;
                for (int i = 0; i < primaryKeys.size(); i++) {
                    if (applyAnd)
                        sb.append(" and ");
                    sb.append(this.primaryKeys.get(i));
                    sb.append("=?");
                    applyAnd = true;
                }
                if (applyAnd)
                    this.updateSQL = sb.toString();
            }
        }
        if (buildDeleteSQL) {
            sb = new StringBuilder();
            sb.append("delete from ");
            sb.append(this.getName());
            sb.append(" where ");
            applyAnd = false;
            for (int i = 0; i < primaryKeys.size(); i++) {
                if (applyAnd)
                    sb.append(" and ");
                sb.append(primaryKeys.get(i));
                sb.append("=?");
                applyAnd = true;
            }
            if (applyAnd)
                this.deleteSQL = sb.toString();
        }
        sb = new StringBuilder();
        sb.append("select 1 as result from ");
        sb.append(this.getName());
        sb.append(" where ");
        applyAnd = false;
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (applyAnd)
                sb.append(" and ");
            sb.append(primaryKeys.get(i));
            sb.append("=?");
            applyAnd = true;
        }
        if (applyAnd)
            this.primaryKeyExistsSQL = sb.toString();
        // Unique Key SQL Building part starts
        LinkedList<Column> uniqueKeyColumns;
        Set<String> uniqueKeyIndexNames = getUniqueKeysIndexNames();
        for (String uniqueKeyIndexName : uniqueKeyIndexNames) {
            uniqueKeyColumns = this.uniqueKeys.get(uniqueKeyIndexName);
            sb = new StringBuilder();
            sb.append("select 1 as result from ");
            sb.append(this.getName());
            sb.append(" where ");
            applyAnd = false;
            for (Column uniqueKeyColumn : uniqueKeyColumns) {
                if (applyAnd) {
                    sb.append(" and ");
                }
                sb.append(uniqueKeyColumn.getName());
                sb.append("=?");
                applyAnd = true;
            }
            if (uniqueKeyExistsSQL == null)
                uniqueKeyExistsSQL = new HashMap<String, String>();
            uniqueKeyExistsSQL.put(uniqueKeyIndexName, sb.toString());
        }
        /*
         * boolean isUniqueAndPrimary;
         * for(int i=0;i<columns.size();i++)
         * {
         * column=columns.get(i);
         * if(column.getIsUnique())
         * {
         * isUniqueAndPrimary=false;
         * for(int k=0;k<primaryKeys.size();k++)
         * {
         * if(primaryKeys.get(k).equals(column.getName()))
         * {
         * isUniqueAndPrimary=true;
         * break;
         * }
         * }
         * if(isUniqueAndPrimary) continue;
         * sb=new StringBuilder();
         * sb.append("select 1 as result from ");
         * sb.append(this.getName());
         * sb.append(" where ");
         * sb.append(column.getName());
         * sb.append("=?");
         * if(uniqueKeyExistsSQL==null) uniqueKeyExistsSQL=new HashMap<String,String>();
         * uniqueKeyExistsSQL.put(column.getName(),sb.toString());
         * }
         * }
         */
        // UniqueKey for Update starts
        int i;
        boolean applyOr;
        for (String uniqueKeyIndexName : uniqueKeyIndexNames) {
            uniqueKeyColumns = this.uniqueKeys.get(uniqueKeyIndexName);
            sb = new StringBuilder();
            sb.append("select 1 as result from ");
            sb.append(this.getName());
            sb.append(" where ");
            applyAnd = false;
            for (Column uniqueKeyColumn : uniqueKeyColumns) {
                if (applyAnd) {
                    sb.append(" and ");
                }
                sb.append(uniqueKeyColumn.getName());
                sb.append("=?");
                applyAnd = true;
            }
            sb.append(" and (");
            applyOr = false;
            for (i = 0; i < primaryKeys.size(); i++) {
                if (applyOr)
                    sb.append(" or ");
                sb.append(primaryKeys.get(i));
                sb.append("<>?");
                applyOr = true;
            }
            sb.append(")");
            if (uniqueKeyExistsSQLForUpdateOperation == null)
                uniqueKeyExistsSQLForUpdateOperation = new HashMap<String, String>();
            uniqueKeyExistsSQLForUpdateOperation.put(uniqueKeyIndexName, sb.toString());
        }
        /*
         * int i;
         * for(int jj=0;jj<columns.size();jj++)
         * {
         * column=columns.get(jj);
         * if(column.getIsUnique())
         * {
         * isUniqueAndPrimary=false;
         * for(int k=0;k<primaryKeys.size();k++)
         * {
         * if(primaryKeys.get(k).equals(column.getName()))
         * {
         * isUniqueAndPrimary=true;
         * break;
         * }
         * }
         * if(isUniqueAndPrimary) continue;
         * sb=new StringBuilder();
         * sb.append("select 1 as result from ");
         * sb.append(this.getName());
         * sb.append(" where ");
         * sb.append(column.getName());
         * sb.append("=?");
         * sb.append(" and (");
         * boolean applyOr;
         * applyOr=false;
         * for(i=0;i<primaryKeys.size();i++)
         * {
         * if(applyOr) sb.append(" or ");
         * sb.append(primaryKeys.get(i));
         * sb.append("<>?");
         * applyOr=true;
         * }
         * sb.append(")");
         * if(uniqueKeyExistsSQLForUpdateOperation==null)
         * uniqueKeyExistsSQLForUpdateOperation=new
         * HashMap<String,String>();
         * uniqueKeyExistsSQLForUpdateOperation.put(column.getName(),sb.toString());
         * }
         * }
         */
        // Unique Key Build SQL Ends
    }

    public String getSelectSQL() throws DMFrameworkException {
        return this.selectSQL;
    }

public String getInsertSQL() throws DMFrameworkException
{
if(insertSQL==null) throw new DMFrameworkException("Insert operation not supported on "+this.className);
return this.insertSQL;
}

public String getUpdateSQL() throws DMFrameworkException
{
if(updateSQL==null) throw new DMFrameworkException("Update operation not supported on "+this.className);
return this.updateSQL;
}

public String getDeleteSQL() throws DMFrameworkException
{
if(deleteSQL==null) throw new DMFrameworkException("Delete operation not supported on "+this.className);
return this.deleteSQL;
}

public String getPrimaryKeyExistsSQL() throws DMFrameworkException
{
if(primaryKeyExistsSQL==null) throw new DMFrameworkException("Exists operation not supported on "+this.className);
return this.primaryKeyExistsSQL;
 }

    public boolean hasUniqueKeys() {
        return uniqueKeyExistsSQL != null && uniqueKeyExistsSQL.size() > 0;
    }

    public boolean hasExportedKeys() {
        return exportedKeys.size() > 0;
    }

    public boolean hasForeignKeys() {
        return foreignKeys.size() > 0;
    }

    public String getUniqueKeyExistsSQL(String uniqueKeyIndexName) {
        return uniqueKeyExistsSQL.get(uniqueKeyIndexName);
    }

    /*
     * public ArrayList<String> getUniqueKeys()
     * {
     * ArrayList<String> uniqueKeys=new ArrayList<String>();
     * if(uniqueKeyExistsSQL==null) return uniqueKeys;
     * uniqueKeys.addAll(uniqueKeyExistsSQL.keySet());
     * return uniqueKeys;
     * }
     */
    public String getUniqueKeyExistsSQLForUpdateOperation(String uniqueKeyIndexName) {
        return uniqueKeyExistsSQLForUpdateOperation.get(uniqueKeyIndexName);
    }

    /*
     * public ArrayList<String> getUniqueKeysForUpdateOperation()
     * {
     * ArrayList<String> uniqueKeys=new ArrayList<String>();
     * if(uniqueKeyExistsSQLForUpdateOperation==null) return uniqueKeys;
     * uniqueKeys.addAll(uniqueKeyExistsSQLForUpdateOperation.keySet());
     * return uniqueKeys;
     * }
     */
    public ArrayList<String> getParentTables() {
        return new ArrayList<String>(foreignKeys.keySet());
    }

    public ArrayList<String> getChildTables() {
        return new ArrayList<String>(exportedKeys.keySet());
    }

    public void buildForeignKeysSQL() {
        Iterator<Map.Entry<String, ForeignKey>> iterator;
        iterator = foreignKeys.entrySet().iterator();
        while (iterator.hasNext()) {
            iterator.next().getValue().buildSQL();
        }
    }

    public void buildExportedKeysSQL() {
        Iterator<Map.Entry<String, ExportedKey>> iterator;
        iterator = exportedKeys.entrySet().iterator();
        while (iterator.hasNext()) {
            iterator.next().getValue().buildSQL();
        }
    }

    public HashMap<String, LinkedList<Column>> getUniqueKeys() {
        return this.uniqueKeys;
    }

    public Set<String> getUniqueKeysIndexNames() {
        return this.uniqueKeys.keySet();
    }

    public void setupUniqueKeys() {
        HashSet<String> uniqueIndexNames = new HashSet<String>();
        LinkedList<Column> uniqueKeyColumnsGroup;
        String uniqueIndexName;
        for (Column column : columns) {
            uniqueIndexName = column.getUniqueIndexName();
            if (uniqueIndexName == null)
                continue;
            if (uniqueIndexNames.contains(uniqueIndexName))
                continue;
            if (uniqueIndexName.equalsIgnoreCase("PRIMARY"))
                continue;
            uniqueIndexNames.add(uniqueIndexName);
        }
        uniqueKeys = new HashMap<String, LinkedList<Column>>();
        Iterator<String> iterator = uniqueIndexNames.iterator();
        while (iterator.hasNext()) {
            uniqueIndexName = iterator.next();
            uniqueKeyColumnsGroup = new LinkedList<Column>();
            for (Column column : columns) {
                if (column.getUniqueIndexName() != null && column.getUniqueIndexName().equals(uniqueIndexName)) {
                    uniqueKeyColumnsGroup.add(column);
                }
            }
            uniqueKeys.put(uniqueIndexName, uniqueKeyColumnsGroup);
        }
    }

    public void setDatabase(Database database) {
        this.database = database;
    }

    public String getArchitecture() {
        return database.getArchitecture();
    }

    public Database getDatabase() {
        return this.database;
    }
}