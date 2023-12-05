package sqlancer.gaussdbm.ast;

public class GaussDBMUnaryPostfixOperation implements GaussDBMExpression {

    private final GaussDBMExpression expression;
    private final UnaryPostfixOperator operator;
    private boolean negate;

    public enum UnaryPostfixOperator {
        IS_NULL, IS_TRUE, IS_FALSE
    }

    public GaussDBMUnaryPostfixOperation(GaussDBMExpression expr, UnaryPostfixOperator op, boolean negate) {
        this.expression = expr;
        this.operator = op;
        this.setNegate(negate);
    }

    public GaussDBMExpression getExpression() {
        return expression;
    }

    public UnaryPostfixOperator getOperator() {
        return operator;
    }

    public boolean isNegated() {
        return negate;
    }

    public void setNegate(boolean negate) {
        this.negate = negate;
    }

    @Override
    public GaussDBMConstant getExpectedValue() {
        boolean val;
        GaussDBMConstant expectedValue = expression.getExpectedValue();
        switch (operator) {
            case IS_NULL:
                val = expectedValue.isNull();
                break;
            case IS_FALSE:
                val = !expectedValue.isNull() && !expectedValue.asBooleanNotNull();
                break;
            case IS_TRUE:
                val = !expectedValue.isNull() && expectedValue.asBooleanNotNull();
                break;
            default:
                throw new AssertionError(operator);
        }
        if (negate) {
            val = !val;
        }
        return GaussDBMConstant.createIntConstant(val ? 1 : 0);
    }

}
