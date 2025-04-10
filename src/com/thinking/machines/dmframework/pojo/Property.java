package com.thinking.machines.dmframework.pojo;

public class Property implements java.io.Serializable, Comparable<Property> {
    private String name;
    private Class type;

    public Property() {
        this.name = "";
        this.type = null;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void setType(Class type) {
        this.type = type;
    }

    public Class getType() {
        return this.type;
    }

    public boolean equals(Object object) {
        if (!(object instanceof Property))
            return false;
        Property another = (Property) object;
        return this.name.equals(another.name);
    }

    public int compareTo(Property property) {
        return this.name.compareTo(property.name);
    }

    public int hashCode() {
        return name.hashCode();
    }
}