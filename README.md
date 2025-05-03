Here's your content converted into a polished `README.md` file format with appropriate markdown structure, added clarity, and light technical enhancements where needed:

---

# Java ORM Framework

## Overview

This project is a lightweight **Object-Relational Mapping (ORM)** framework developed in Java, inspired by Hibernate. It is designed to simplify database interactions by abstracting SQL queries and allowing object-level operations on relational data.

This framework is ideal for small to mid-sized applications and serves as a drop-in replacement for Hibernate in simpler use cases. It bridges the gap between Java-based business logic and relational databases through runtime introspection and annotation-based mapping.

> Developed under the guidance of **@ThinkingMachines** as part of an internship project.

---

## ✨ Key Features

- **Entity Mapping**  
  Annotated POJOs are mapped to database tables using custom annotations like `@Entity`, `@Table`, `@Column`, `@Id`, etc.

- **Dynamic SQL Generation**  
  Uses Java Reflection APIs to generate SQL queries at runtime based on annotation metadata.

- **Type-Safe CRUD Operations**  
  Provides a set of type-safe methods such as:
  - `save(Object entity)`
  - `update(Object entity)`
  - `delete(Object entity)`
  - `findById(Class<T> entityClass, Object id)`
  - `findAll(Class<T> entityClass)`

- **Custom Validation Engine**  
  Incorporates multiple validation classes to enforce rules like primary key uniqueness, required fields, foreign key existence, and more.

- **Transaction Management**  
  Basic support for managing commit and rollback operations via a simplified transaction API.

- **Connection Pooling**  
  Efficient database connection handling using either a custom or third-party connection pool manager.

- **Exception Handling**  
  Includes custom exceptions such as:
  - `DMFrameworkException`
  - `EntityNotFoundException`
  - `ValidatorException`
  These enable more expressive debugging and clearer fault identification.

- **XML-Based Configuration**  
  Database parameters and schema mappings are externalized via an easy-to-edit XML file (`TMDMFramework.xml`).

---

## 📂 Project Structure

```text
com/
└── thinking/
    └── machine/
        └── dmframework/
            ├── DataManager.java
            ├── Entity.java
            ├── EntityManager.java
            ├── TMDMFramework.xml

            ├── annotations/
            │   ├── Column.java
            │   ├── Display.java
            │   ├── PrimaryKey.java
            │   ├── Required.java
            │   ├── Sequence.java
            │   ├── Sort.java
            │   ├── Table.java
            │   ├── Unique.java
            │   ├── View.java
            │   └── Width.java

            ├── query/
            │   ├── Clause.java
            │   ├── Expression.java
            │   ├── LogicalOperator.java
            │   ├── Operators.java
            │   ├── QueryImplementor.java
            │   ├── Select.java
            │   ├── SelectWrapper.java
            │   └── Where.java

            ├── exceptions/
            │   ├── DMFrameworkException.java
            │   ├── ExceptionIterator.java
            │   ├── ExceptionsIterator.java
            │   └── ValidatorException.java

            ├── validators/
            │   ├── ChildValidator.java
            │   ├── ChildWrapper.java
            │   ├── KeyValidator.java
            │   ├── OverflowValidator.java
            │   ├── ParentValidator.java
            │   ├── ParentWrapper.java
            │   ├── PrimaryKeyValidator.java
            │   ├── RequiredValidator.java
            │   ├── UniqueKeyValidator.java
            │   ├── UniqueKeyValidatorForUpdateOperation.java
            │   ├── UniqueKeyWrapper.java
            │   └── Validator.java

            ├── pojo/
            │   ├── Column.java
            │   ├── Database.java
            │   ├── DataType.java
            │   ├── ExportedKey.java
            │   ├── ForeignKey.java
            │   ├── MethodWrapper.java
            │   ├── Pair.java
            │   ├── Property.java
            │   ├── Table.java
            │   └── View.java

            ├── utilities/
            │   ├── ConfigurationUtility.java
            │   ├── DatabaseUtility.java
            │   ├── Types.java
            │   └── Utilities.java

            └── dml/
                ├── DeleteWrapper.java
                ├── InsertWrapper.java
                ├── SequenceWrapper.java
                └── UpdateWrapper.java
```

---

## 🛠 Technologies Used

- Java (Reflection, Annotations, JDBC)
- XML (for configuration)
- Apache Commons Lang (for string operations)
- Custom Exception and Validation Framework

---

## 🔧 Setup Instructions

1. Clone the repository.
2. Place your annotated entity classes in the correct package.
3. Configure your `TMDMFramework.xml` file with appropriate database credentials and mapping.
4. Use `EntityManager` and `DataManager` classes to perform data operations.

---

## ✅ Example Usage

```java
@Entity
@Table(name = "users")
public class User {
    @PrimaryKey
    @Column(name = "id")
    private int id;

    @Column(name = "username")
    @Required
    private String username;

    @Column(name = "email")
    @Unique
    private String email;
}

// Using the framework
EntityManager em = new EntityManager();
User user = em.findById(User.class, 1);
```
## ⚙️ Core Functionalities

### 🧩 Annotation-Based Mapping

The framework features a **metadata-driven annotation system**, enabling declarative mapping between Java classes and relational schemas, mimicking Hibernate-style design.

#### Class-Level Annotations
- `@Table`, `@View`: Binds a class to a relational database table or view.

#### Field-Level Annotations
- `@Column`: Maps class fields to table columns with additional metadata (e.g., type, width).
- `@PrimaryKey`, `@Unique`, `@Required`: Enforce schema-level constraints through runtime validation.
- `@Sequence`: Supports auto-incremented keys using database sequences.
- `@Sort`, `@Width`, `@Display`: Provide UI hints for rendering, sorting, and presentation logic.

This system enables reflective analysis of object structure, allowing automatic SQL query generation and validation during runtime.

---

### 🚨 Custom Exception Handling

The ORM framework defines a layered exception handling model to capture and report both system-level and application-specific failures clearly and consistently.

#### Main Exception Classes
- **`DMFrameworkException`**  
  A catch-all exception for critical runtime issues such as JDBC failures, query execution problems, or metadata access errors. This abstracts low-level exceptions into clear messages for developers.

- **`ValidatorException`**  
  Thrown when entity constraints are violated (e.g., missing required fields, duplicate unique keys, invalid schema mappings). Often raised before query execution.

#### Supporting Utilities
- `ExceptionIterator`, `ExceptionsIterator`: Used for batch validation and error collection, particularly when validating lists of entities.

All exceptions include detailed contextual information such as:
- Field name
- Failing value
- Entity class

This design provides strong debugging support and ensures precise feedback to the developer.

---

### 🏗️ Custom Query Builder

A robust **query builder engine** enables developers to construct SQL-like operations using expressive, type-safe Java method chains—without writing any raw SQL or `PreparedStatement` code.

#### Supported Operations
- CRUD (`add`, `update`, `delete`, `find`)
- Conditional filtering and logical composition

#### Composable Query Elements
- **Clauses**: `WHERE`, `ORDER BY`, `GROUP BY`
- **Operators**: `=`, `!=`, `LIKE`, `IN`, `BETWEEN`, etc.
- **Logical Operators**: `AND`, `OR`, `NOT`
- **Wrappers/Expressions**: Nested conditions, subqueries

#### Internal Architecture
- `QueryImplementor`, `SelectWrapper`, and `Where` classes work together to build parameterized, secure SQL queries dynamically.
- Uses annotation metadata and reflection to resolve field mappings.
- Prevents SQL injection via auto-generated prepared statements.
- Supports multi-entity joins through relationship annotations.

This fluent API brings clarity and modularity to complex query construction, improving maintainability and reducing developer error.

---

### ✅ Complex Validator System

A highly modular **validation framework** ensures data correctness, schema compliance, and semantic integrity before query execution.

Validators are automatically triggered during `save`, `update`, and custom query execution to enforce business and database rules.

#### 🧒 ChildValidator
- `validateChild()`: Validates linked child entities.
- `validateByPrimaryKey()`, `validateChildByPrimaryKey()`: Checks foreign key references for JOIN operations.

#### 🔑 KeyValidator
- `validateBeforeInsertion()`: Verifies compound key and inter-field constraints.
- `validateByPrimaryKey()`: Ensures primary key integrity before operation.

#### 📏 OverflowValidator
- `validate()`: Checks for overflow against defined width constraints (`@Width`), ensuring no data truncation.

#### 🧑‍🍼 ParentValidator
- `validate()`: Ensures foreign parent entities exist before establishing references.

#### 🆔 PrimaryKeyValidator
- `validate()`: Confirms primary key fields are present and properly annotated.

#### 📌 RequiredValidator
- `validate()`: Enforces non-null checks for fields annotated with `@Required`.

#### 🔐 UniqueKeyValidator / UniqueKeyValidatorForUpdateOperation
- `validate()`: Prevents duplicate entries in fields marked with `@Unique`.
- The update version skips the current record when checking for uniqueness conflicts.

#### ⚙️ Architecture Highlights
- All validators use reflection and annotation metadata—no hardcoded rules.
- Ensures validation occurs **before** SQL execution.
- Maintains separation between **persistence** and **validation logic**.
- Helps catch issues early in the development lifecycle.

This validator system improves correctness, reduces database-side errors, and enhances framework portability and maintainability.


---

## 🤝 Acknowledgements

Special thanks to **@ThinkingMachines** for mentorship and project guidance.
