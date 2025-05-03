### **Custom Query Builder**

The framework provides a fully extensible **custom query builder** that abstracts away the need for writing raw SQL or `PreparedStatement` code. Instead, developers can construct queries using high-level, type-safe Java APIs.

Users can perform standard CRUD operations (`add`, `update`, `delete`, `find`) through intuitive method calls, with support for dynamic condition building using entity metadata and annotation-driven mappings.

The query engine allows building complex queries by chaining components such as:

- **Clauses** (`WHERE`, `ORDER BY`, `GROUP BY`)    
- **Operators** (`=`, `!=`, `>`, `<`, `LIKE`, `IN`, `BETWEEN`)    
- **Logical Operators** (`AND`, `OR`, `NOT`)    
- **Expressions and Wrappers**: Encapsulate nested conditions and subqueries.    

Internally, the framework translates the query structure into optimized, parameterized SQL to prevent injection attacks. It uses reflection to resolve field-to-column mappings and supports multiple entity joins via annotated relationships.

The `QueryImplementor`, `SelectWrapper`, and `Where` classes work together to construct SQL statements dynamically, while maintaining separation of concerns between query structure and execution logic.

This modular and fluent design empowers developers to compose sophisticated queries without sacrificing maintainability or safety.
