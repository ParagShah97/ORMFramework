package com.thinking.machines.dmframework.pojo;

import java.util.*;

import com.thinking.machines.dmframework.annotations.Table;

public class Column implements java.io.Serializable, Comparable<Column> {
    private String name;
    private Property property;
    private boolean isNullable;
    private Object defaultValue;
    private boolean isAutoIncrementEnabled;
    private boolean isSequenceAssociated;
    private String nameOfSequence;
    private boolean isUnique;
    private String uniqueIndexName;
    private int width;
    private int decimalPrecision;
    private int type;
    private String displayName;
    private Table table;
    private View view;
    private boolean applyPadding;

    public Column() {
        this.name = "";
        this.property = null;
        this.isNullable = false;
        this.defaultValue = null;
        this.isAutoIncrementEnabled = false;
        this.isUnique = false;
        this.width = 0;
        this.decimalPrecision = 0;
        this.type = 0;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void setProperty(Property property) {
        this.property = property;
    }

    public Property getProperty() {
        return this.property;
    }

    public void setIsNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }

    public boolean getIsNullable() {
        return this.isNullable;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public Object getDefaultValue() {
        return this.defaultValue;
    }

    public void setIsAutoIncrementEnabled(boolean isAutoIncrementEnabled) {
        this.isAutoIncrementEnabled = isAutoIncrementEnabled;
    }

    public boolean getIsAutoIncrementEnabled() {
        return this.isAutoIncrementEnabled;
    }

    public void setIsSequenceAssociated(boolean isSequenceAssociated) {
        this.isSequenceAssociated = isSequenceAssociated;
    }

    public boolean getIsSequenceAssociated() {
        return this.isSequenceAssociated;
    }

    public void setNameOfSequence(String nameOfSequence) {
        this.nameOfSequence = nameOfSequence;
    }

    public String getNameOfSequence() {
        return this.nameOfSequence;
    }

    public void setIsUnique(boolean isUnique) {
        this.isUnique = isUnique;
    }

    public boolean getIsUnique() {
        return this.isUnique;
    }

    public void setUniqueIndexName(String uniqueIndexName) {
        this.uniqueIndexName = uniqueIndexName;
    }

    public String getUniqueIndexName() {
        return this.uniqueIndexName;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public int getWidth() {
        return this.width;
    }

    public void setDecimalPrecision(int decimalPrecision) {
        this.decimalPrecision = decimalPrecision;
    }

    public int getDecimalPrecision() {
        return this.decimalPrecision;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getType() {
        return this.type;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return this.table;
    }

    public boolean isPartOfPrimaryKey() {
        for (String pk : table.getPrimaryKeys()) {
            if (pk.equals(this.name))
                return true;
        }
        return false;
    }

    public void setView(View view) {
        this.view = view;
    }

    public View getView() {
        return this.view;
    }

    public boolean isInTable() {
        return this.table != null;
    }

    public boolean isInView() {
        return this.view != null;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return this.displayName;
    }

    public String getArchitecture() {
        if (isInView())
            return this.view.getArchitecture();
        return this.table.getArchitecture();
    }

    public void applyPadding(boolean applyPadding) {
        this.applyPadding = applyPadding;
    }

    public boolean applyPadding() {
        return this.applyPadding;
    }

    public boolean equals(Object object) {
        if (!(object instanceof Column))
            return false;
        Column another = (Column) object;
        return this.name.equals(another.name);
    }

    public int compareTo(Column column) {
        return this.name.compareTo(column.name);
    }

    public int hashCode() {
        // we are assuming that the system will have info of on database only
        // and table name and view name will differ 100%
        if (isInTable())
            return Objects.hash(this.getTable().getName(), this.name);
        return Objects.hash(this.getView().getName(), this.name);
    }
}