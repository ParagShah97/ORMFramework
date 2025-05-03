### **Complex Validator System**

The framework features a **modular and extensible validation system** designed to validate complex queries before they are executed. This system ensures data integrity, constraint enforcement, and semantic correctness of user-defined operations like nested transactions, subqueries, and joins—without requiring users to manually handle SQL-level validation.

Each validator encapsulates a specific aspect of data consistency and schema compliance, triggered automatically during the query-building and execution phases.

#### 🔍 **ChildValidator**
- **`validateChild()`**: Ensures the integrity of child entity relationships.    
- **`validateByPrimaryKey()` / `validateChildByPrimaryKey()`**: Confirms that related child records exist and are correctly referenced via foreign keys, enforcing referential integrity in JOIN or cascading operations.    

#### 🔑 **KeyValidator**
- **`validateBeforeInsertion()`**: Verifies composite key constraints and inter-field dependencies prior to `INSERT`.    
- **`validateByPrimaryKey()`**: Checks for primary key presence and value correctness, preventing accidental data duplication or update errors.
    

#### 📈 **OverflowValidator**
- **`validate()`**: Validates whether the data exceeds field width limits as specified by the `@Width` or SQL type constraints, protecting against data truncation and runtime exceptions.    

#### 🔗 **ParentValidator**
- **`validate()`**: Ensures parent entities exist and are accessible before performing operations involving foreign key references or hierarchy mappings.    

#### 🆔 **PrimaryKeyValidator**
- **`validate()`**: Validates the presence and correctness of `@PrimaryKey` annotations, required for uniquely identifying rows and enforcing entity identity.
    

#### 📌 **RequiredValidator**
- **`validate()`**: Enforces `@Required` fields are populated before `INSERT` or `UPDATE`, throwing a `ValidatorException` if any non-nullable column is missing data.
    

#### 🔐 **UniqueKeyValidator / UniqueKeyValidatorForUpdateOperation**
- **`validate()`**: Ensures that fields marked `@Unique` do not violate database uniqueness constraints during both insert and update operations. The update-specific version accounts for excluding the current row during uniqueness checks.    

This system supports **pre-execution validation**, helping developers catch schema violations, logical errors, or misuse of entities early in the development cycle. It uses reflection and the metadata layer to map Java properties to schema constraints dynamically, thus eliminating the need for hardcoded validations and improving code portability, correctness, and maintainability.
