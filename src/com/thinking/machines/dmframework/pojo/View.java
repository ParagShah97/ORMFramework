package com.thinking.machines.dmframework.pojo;
import java.util.*;
import com.thinking.machines.dmframework.utilities.*;
import com.thinking.machines.dmframework.exceptions.*;
public class View implements java.io.Serializable,Comparable<View>
{
private String name;
private List<Column> columns;
private String className;
private Database database;
private String displayName;
private String selectSQL;
public View()
{
this.name="";
this.columns=new ArrayList<Column>();
this.className="";
}
public void setDatabase(Database database)
{
this.database=database;
}
public void setName(String name)
{
this.name=name;
setClassName(Utilities.getJavaClassName(this.name));
 }
 public String getName()
 {
 return this.name;
 }
 public void setColumns(List<Column> columns)
 {
 this.columns=columns;
 }
 public List<Column> getColumns()
 {
 return this.columns;
 }
 public void setClassName(String className)
 {
 this.className=className;
 }
 public String getClassName()
 {
 return this.className;
 }
 public boolean equals(Object object)
 {
 if(!(object instanceof View)) return false;
 View another=(View)object;
 return this.name.equals(another.name);
 }
 public int compareTo(View view)
 {
 return this.name.compareTo(view.name);
 }
 public int hashCode()
 {
 return this.name.hashCode();
 }
 public void addColumn(Column column)
 {
 columns.add(column);
 }
 public String getArchitecture()
 {
 return this.database.getArchitecture();
 }
 public void setDisplayName(String displayName)
 {
 this.displayName=displayName;
}
public String getDisplayName()
{
return this.displayName;
}
public void buildSQL()
{
selectSQL="select * from "+this.getName();
}
public String getSelectSQL() throws DMFrameworkException
{
return this.selectSQL;
}
}