package com.thinking.machines.dmframework.validators;

import com.thinking.machines.dmframework.exceptions.*;

public interface Validator {
    public void validate(Object object) throws DMFrameworkException, ValidatorException;

    default public void validateBeforeInsertion(Object object) throws DMFrameworkException, ValidatorException {
        ValidatorException validatorException = new ValidatorException();
        validatorException.add("generic", "validateBeforeInsertion not yet implemented");
        throw validatorException;
    }
}