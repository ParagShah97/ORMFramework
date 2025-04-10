package com.thinking.machines.dmframework.annotations;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Sort {
    public int priority() default 1;

    public boolean descending() default false;
}