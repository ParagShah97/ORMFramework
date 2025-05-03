package com.thinking.machines.dmframework.query;

public class LogicalOperator<T> {
    private QueryImplementor queryImplementor;
    private Clause clause;
    private String logicalOperator;

    public LogicalOperator(QueryImplementor queryImplementor, String logicalOperator, Clause clause) {
        this.queryImplementor = queryImplementor;
        this.clause = clause;
        this.logicalOperator = logicalOperator;
    }

    public Expression eq(Object rightOperand) {
        clause.setOperator(Operators.eq);
        clause.setRightOperand(rightOperand);
        Expression expression = new Expression(queryImplementor, logicalOperator, clause);
        return expression;
    }

    public Expression<T> ne(Object rightOperand) {
        clause.setOperator(Operators.ne);
        clause.setRightOperand(rightOperand);
        Expression expression = new Expression(queryImplementor, logicalOperator, clause);
        return expression;
    }

    public Expression<T> gt(Object rightOperand) {
        clause.setOperator(Operators.gt);
        clause.setRightOperand(rightOperand);
        Expression expression = new Expression(queryImplementor, logicalOperator, clause);
        return expression;
    }

    public Expression<T> ge(Object rightOperand) {
        clause.setOperator(Operators.ge);
        clause.setRightOperand(rightOperand);
        Expression expression = new Expression(queryImplementor, logicalOperator, clause);
        return expression;
    }

    public Expression<T> lt(Object rightOperand) {
        clause.setOperator(Operators.lt);
        clause.setRightOperand(rightOperand);
        Expression expression = new Expression(queryImplementor, logicalOperator, clause);
        return expression;
    }

    public Expression<T> le(Object rightOperand) {
        clause.setOperator(Operators.le);
        clause.setRightOperand(rightOperand);
        Expression expression = new Expression(queryImplementor, logicalOperator, clause);
        return expression;
    }
}