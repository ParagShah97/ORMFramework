package com.thinking.machines.dmframework.validators;

import com.thinking.machines.dmframework.exceptions.*;
import java.sql.*;

public interface KeyValidator {
    public void validate(Connection connection, Object object, boolean throwExceptionIfExists)
            throws DMFrameworkException, ValidatorException;

    default public void validateBeforeInsertion(Connection connection, Object object, boolean throwExceptionIfExists)
            throws DMFrameworkException, ValidatorException {
        ValidatorException validatorException = new ValidatorException();
        validatorException.add("generic", "validateBeforeInsertion not yet implemented");
        throw validatorException;
    }

    default public void validateByPrimaryKey(Connection connection, boolean throwExceptionIfExists,
            Object... primaryKey) throws DMFrameworkException, ValidatorException {
        ValidatorException validatorException = new ValidatorException();
        validatorException.add("generic", "validateByPrimaryKey not yet implemented");
        throw validatorException;
    }
}