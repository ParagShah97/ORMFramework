package com.thinking.machines.dmframework.query;

import com.thinking.machines.dmframework.exceptions.*;
import java.util.*;

public interface QueryImplementor<T> {
    public List<T> query() throws DMFrameworkException;

    public void addExpression(Expression<T> expression);
}