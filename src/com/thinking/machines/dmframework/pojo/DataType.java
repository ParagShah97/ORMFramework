package com.thinking.machines.dmframework.pojo;
public class DataType implements java.io.Serializable
{
private Class type;
private Class primitiveType;
private String defaultValue;
private String setterGetterMethodName;
private boolean trimString;
public DataType()
{
this.type=null;
this.defaultValue="";
this.setterGetterMethodName="";
this.trimString=false;
}
public void setType(Class type)
{
this.type=type;
}
public void setTrimString(boolean trimString)
{
this.trimString=trimString();
}
public boolean trimString()
{
return this.trimString();
}
public Class getType()
{
return this.type;
}
public void setPrimitiveType(Class primitiveType)
{
this.primitiveType=primitiveType;
}
public Class getPrimitiveType()
{
return this.primitiveType;
}
public void setDefaultValue(String defaultValue)
{
this.defaultValue=defaultValue;
}
public String getDefaultValue()
{
return this.defaultValue;
}
public void setSetterGetterMethodName(String setterGetterMethodName)
{
this.setterGetterMethodName=setterGetterMethodName;
}
public String getSetterGetterMethodName()
{
    return this.setterGetterMethodName;
}
public boolean equals(Object object)
{
if(!(object instanceof DataType)) return false;
DataType another=(DataType)object;
return this.type.equals(another.type);
}
public int hashCode()
{
return this.type.hashCode();
}
}