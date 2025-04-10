package com.thinking.machines.dmframework.pojo;

import java.io.*;
import java.lang.reflect.*;
import com.thinking.machines.dmframework.utilities.*;
import com.thinking.machines.dmframework.exceptions.*;
import com.thinking.machines.dmframework.utilities.*;
import org.apache.commons.lang3.*;
import org.apache.commons.lang3.text.*;

public class MethodWrapper implements Serializable {
    private Method method;
    private String property;
    private String spacedProperty;
    private Column column;

 public MethodWrapper(Method method,String property,Column column)
 {
 this.method=method;
 this.property=property;
 this.column=column;
 if(column.getDisplayName()==null || column.getDisplayName().trim().length()==0)
 {
 this.spacedProperty=WordUtils.uncapitalize(StringUtils.join(StringUtils.splitByCharacterTypeCamelC
 ase(property)," "));
 }
 else
 {
 this.spacedProperty=column.getDisplayName();
 }
 }

    public Method getMethod() {
        return this.method;
    }

    public String getProperty() {
        return this.property;
    }

    public void setSpacedProperty(String spacedProperty) {
        this.spacedProperty = spacedProperty;
    }

    public String getSpacedProperty() {
        return this.spacedProperty;
    }

    public String getCapitalizedSpacedProperty() {
        return StringUtils.capitalize(spacedProperty);
    }

    public Column getColumn() {
        return this.column;
    }

    public DataType getDataType() throws DMFrameworkException {
        return Types.getDataType(column.getType());
    }

    public Object invoke(Object object, Object... arguments) throws java.lang.IllegalAccessException,
            java.lang.IllegalArgumentException, java.lang.reflect.InvocationTargetException {
        return method.invoke(object, arguments);
    }
}