## Custom Annotation System – Metadata-Driven ORM
As part of the ORM framework, I designed and implemented a robust custom annotation system in Java that enables declarative mapping between Java classes and relational database schema. These annotations inject metadata at the class, field, and method levels, allowing the framework to dynamically generate and validate SQL queries using Java Reflection and Annotation Processing.

### Key Technical Highlights:

#### Class-Level Annotations
Used to define the structural mapping of a Java class to a database table or view.

@Table(name = "table_name"):
Binds a Java class to a relational table. The name parameter links it directly to the corresponding table in the database.

@View(name = "view_name"):
Indicates that the entity maps to a database view instead of a table, supporting read-only access patterns.

#### Field/Property-Level Annotations
Used to describe the mapping and constraints of class attributes to database columns.

@Column(name = "column_name", type = "VARCHAR", width = 50):
Binds a Java field to a specific column, supporting additional metadata such as SQL type and width.

@PrimaryKey:
Marks a field as the primary key. The framework uses this to generate WHERE clauses for updates and deletes, and to enforce uniqueness in validation.

@Unique:
Ensures that the column value is unique across rows, and performs uniqueness validation during insert and update operations.

@Required:
Marks the column as non-nullable. The validator module enforces this at runtime prior to any database call.

@Sequence(name = "seq_name"):
Specifies the sequence generator for auto-incrementing fields, enabling database-agnostic ID generation strategies.

@Sort(priority = n):
Used to define default sorting behavior for result sets when queries are built dynamically.

@Width(n):
Provides UI hints or constraints for column display width, useful in tools built on top of the ORM (e.g., admin panels or code generators).

@Display(name = "Label"):
Attaches human-readable labels to fields for UI generation or documentation purposes.

#### Method-Level Annotations (Pluggable in Future Versions)
Though currently minimal, the framework is designed to be extensible to allow annotations on getter/setter methods for advanced scenarios like computed fields or lifecycle hooks (@PreInsert, @PostUpdate).

### Technical Backbone:
Annotation Retention:
All annotations use @Retention(RetentionPolicy.RUNTIME) so they are available at runtime through Java Reflection.

Annotation Targeting:
Controlled via @Target (e.g., ElementType.FIELD, ElementType.TYPE) to restrict usage context and improve clarity.

Dynamic Metadata Extraction:
A centralized Metadata Parser scans entities during application bootstrap and populates internal representations (like Table, Column, Property objects in the pojo package), which are used in query generation and validation.

Validation & Error Reporting:
Each annotation contributes to a pipeline of runtime checks. For instance, fields marked @Required and @Unique are validated in the validators module before any persistence is attempted. Violations throw framework-specific exceptions (ValidatorException, DMFrameworkException) with descriptive messages.
