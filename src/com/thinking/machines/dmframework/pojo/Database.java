package com.thinking.machines.dmframework.pojo;

import java.util.*;

import com.thinking.machines.dmframework.annotations.Table;

public class Database implements java.io.Serializable, Comparable<Database> {
    private String name;
    private List<Table> tables;
    private List<View> views;
    private List<String> sequences;
    private String architecture;

    public Database() {
        this.name = "";
        this.tables = null;
        this.views = null;
        this.sequences = null;
    }

    public void setArchitecture(String architecture) {
        this.architecture = architecture;
    }

    public String getArchitecture() {
        return this.architecture;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
        for (Table t : tables) {
            t.setDatabase(this);
        }
    }

    public List<Table> getTables() {
        return this.tables;
    }

    public void setSequences(List<String> sequences) {
        this.sequences = sequences;
    }

    public List<String> getSequences() {
        return this.sequences;
    }

    public boolean containsSequence(String sequenceName) {
        if (this.sequences == null)
            return false;
        for (String sequence : sequences) {
            if (sequence.equalsIgnoreCase(sequenceName))
                return true;
        }
        return false;
    }

    public Table getTable(String name) {
        for (Table t : tables) {
            if (t.getName().equalsIgnoreCase(name))
                return t;
        }
        return null;
    }

    public void setViews(List<View> views) {
        this.views = views;
        for (View v : views) {
            v.setDatabase(this);
        }
    }

    public List<View> getViews() {
        return this.views;
    }

    public View getView(String name) {
        for (View v : views) {
            if (v.getName().equalsIgnoreCase(name))
                return v;
        }
        return null;
    }

    public boolean equals(Object object) {
        if (!(object instanceof Database))
            return false;
        Database another = (Database) object;
        return this.name.equals(another.name);
    }

    public int compareTo(Database database) {
        return this.name.compareTo(database.name);
    }

    public int hashCode() {
        return this.name.hashCode();
    }
}