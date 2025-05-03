package com.thinking.machines.dmframework.query;

public class Clause {
    private String leftOperand;
    private Object rightOperand;
    private Integer operator;

    public Clause() {
        this.leftOperand = null;
        this.rightOperand = null;
        this.operator = null;
    }

    public void setLeftOperand(String leftOperand) {
        this.leftOperand = leftOperand;
    }

    public void setRightOperand(Object rightOperand) {
        this.rightOperand = rightOperand;
    }

    public void setOperator(Integer operator) {
        this.operator = operator;
    }

    public String getLeftOperand() {
        return this.leftOperand;
    }

    public Object getRightOperand() {
        return this.rightOperand;
    }

    public Integer getOperator() {
        return this.operator;
    }
}