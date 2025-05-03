### **Custom Exception Handling**

The ORM framework includes a structured custom exception hierarchy to handle various runtime and logical failures with precision and clarity.

- **`DMFrameworkException`**:  
    A top-level unchecked exception used to encapsulate all critical errors arising during database interaction—such as connection failures, invalid SQL execution, or metadata retrieval issues. It ensures that low-level JDBC exceptions are abstracted into meaningful, framework-specific messages for the developer.
    
- **`ValidatorException`**:  
    Thrown during the validation phase when entity fields violate constraints defined by annotations like `@Required`, `@Unique`, or `@PrimaryKey`. It also handles schema mismatches and misconfigured entity definitions (e.g., missing mappings or sequence misalignment).
    
- **Support Classes**:
    
    - `ExceptionIterator` and `ExceptionsIterator` allow batched exception handling, especially when validating large datasets.
        
    - All exceptions support detailed context (e.g., field name, entity class, failing value) to aid debugging.
        
This layered exception model provides granular control, separates validation from persistence errors, and improves observability by wrapping native exceptions with domain-specific context.
