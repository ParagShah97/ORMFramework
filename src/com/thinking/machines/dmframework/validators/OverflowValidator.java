package com.thinking.machines.dmframework.validators;

import org.apache.log4j.*;
import com.thinking.machines.dmframework.exceptions.*;
import com.thinking.machines.dmframework.pojo.*;
import com.thinking.machines.dmframework.utilities.*;
import java.util.*;
import java.lang.reflect.*;
import java.math.*;

public class OverflowValidator implements Validator {
    private final static Logger logger = Logger.getLogger(OverflowValidator.class);
    private ArrayList<MethodWrapper> getterMethods;

    public OverflowValidator(ArrayList<MethodWrapper> getterMethods) {
        this.getterMethods = getterMethods;
    }

    public void validate(Object object) throws DMFrameworkException, ValidatorException {
        ValidatorException validatorException = new ValidatorException();
        Column column;
        Object returnedValue;
        int width;
        int decimalPrecision;
        for (MethodWrapper methodWrapper : getterMethods) {
            try {
                returnedValue = methodWrapper.invoke(object);
                column = methodWrapper.getColumn();
                width = column.getWidth();
                decimalPrecision = column.getDecimalPrecision();
                if (returnedValue instanceof String) {
                    if (((String) returnedValue).trim().length() > width) {
                        validatorException.add(methodWrapper.getColumn().getProperty().getName(), "Length of "
                                + methodWrapper.getSpacedProperty() + " should not exceed " + width + " characters.");
                        // throw new DMFrameworkException("Length of
                        // "+methodWrapper.getSpacedProperty()+" should not exceed "+width+"
                        // characters.");
                    }
                    continue;
                }
                if (returnedValue instanceof Long) {
                    if (Utilities.getNumberOfDigits((Long) returnedValue) > width) {
                        validatorException.add(methodWrapper.getColumn().getProperty().getName(), "Number of digits in "
                                + methodWrapper.getSpacedProperty() + " should not exceed " + width + ".");
                        // throw new DMFrameworkException("Number of digits in
                        // "+methodWrapper.getSpacedProperty()+" should not exceed "+width+".");
                    }
                    continue;
                }
                if (returnedValue instanceof Integer) {
                    if (Utilities.getNumberOfDigits((Integer) returnedValue) > width) {
                        validatorException.add(methodWrapper.getColumn().getProperty().getName(), "Number of digits in "
                                + methodWrapper.getSpacedProperty() + " should not exceed " + width + ".");
                        // throw new DMFrameworkException("Number of digits in
                        // "+methodWrapper.getSpacedProperty()+" should not exceed "+width+".");
                    }
                    continue;
                }
                if (returnedValue instanceof BigDecimal) {
                    Pair<Integer, Pair<Integer, Integer>> pair;
                    pair = Utilities.getBigDecimalLength((BigDecimal) returnedValue);
                    int totalLength = pair.getFirst();
                    int numberOfIntegerDigits = pair.getSecond().getFirst();
                    int numberOfFractionalDigits = pair.getSecond().getSecond();
                    int numberOfAllowedIntegerDigits = 0;
                    int numberOfAllowedFractionalDigits = 0;
                    if (decimalPrecision > 0) {
                        numberOfAllowedIntegerDigits = width - decimalPrecision;
                        numberOfAllowedFractionalDigits = width - numberOfAllowedIntegerDigits;
                    } else {
                        numberOfAllowedIntegerDigits = width;
                    }
                    if (numberOfIntegerDigits > numberOfAllowedIntegerDigits) {
                        validatorException.add(methodWrapper.getColumn().getProperty().getName(),
                                "Number of digits in integer part of " + methodWrapper.getSpacedProperty()
                                        + " should not exceed " + numberOfAllowedIntegerDigits + ".");
                        // throw new DMFrameworkException("Number of digits in integer part of
                        // "+methodWrapper.getSpacedProperty()+" should not exceed
                        // "+numberOfAllowedIntegerDigits+".");
                    }
                    if (numberOfFractionalDigits > numberOfAllowedFractionalDigits) {
                        validatorException.add(methodWrapper.getColumn().getProperty().getName(),
                                "Number of digits in fractional part of " + methodWrapper.getSpacedProperty()
                                        + " should not exceed " + numberOfAllowedFractionalDigits + ".");
                        // throw new DMFrameworkException("Number of digits in fractional part of
                        // "+methodWrapper.getSpacedProperty()+" should not exceed
                        // "+numberOfAllowedFractionalDigits+".");
                    }
                }
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException commonException) {
                logger.error(commonException.getMessage());
                throw new DMFrameworkException(commonException.getMessage());
            }
        }
        if (validatorException.hasExceptions())
            throw validatorException;
    }
}