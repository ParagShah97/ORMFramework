package com.thinking.machines.dmframework.pojo;

import java.util.*;

public class ForeignKey {
    private HashMap<Integer, Pair<Column, Column>> columnMappings;
    private String mappedToTable;
    private String existsSQL;

    public ForeignKey() {
        this.columnMappings = new HashMap<Integer, Pair<Column, Column>>();
        this.mappedToTable = "";
    }

    public void setColumnMappings(HashMap<Integer, Pair<Column, Column>> columnMappings) {
        this.columnMappings = columnMappings;
    }

    public HashMap<Integer, Pair<Column, Column>> getColumnMappings() {
        return this.columnMappings;
    }

    public void setMappedToTable(String mappedToTable) {
        this.mappedToTable = mappedToTable;
    }

    public String getMappedToTable() {
        return this.mappedToTable;
    }

    public void addColumnMapping(int sequenceNumber, Pair<Column, Column> columnMapping) {
        this.columnMappings.put(sequenceNumber, columnMapping);
    }

    public Pair<Column, Column> getColumnMapping(int sequenceNumber) {
        return this.columnMappings.get(sequenceNumber);
    }

    public int getColumnMapingsSize() {
        return this.columnMappings.size();
    }

    public void buildSQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("select 1 as result from ");
        sb.append(mappedToTable);
        sb.append(" where ");
        int i;
        boolean applyAnd = false;
        for (i = 1; i <= columnMappings.size(); i++) {
            if (applyAnd)
                sb.append(" and ");
            sb.append(columnMappings.get(i).getSecond().getName());
            sb.append("=?");
            applyAnd = true;
        }
        existsSQL = sb.toString();
    }

    public String getExistsSQL() {
        return this.existsSQL;
    }
}