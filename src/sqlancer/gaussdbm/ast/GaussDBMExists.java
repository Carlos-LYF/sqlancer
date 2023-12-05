package sqlancer.gaussdbm.ast;

public class GaussDBMExists implements GaussDBMExpression {

    private final GaussDBMExpression expr;
    private final GaussDBMConstant expected;

    public GaussDBMExists(GaussDBMExpression expr) {
        this.expr = expr;
        this.expected = expr.getExpectedValue();
        if (expected == null) {
            throw new AssertionError();
        }
    }

    public GaussDBMExpression getExpr() {
        return expr;
    }

    @Override
    public GaussDBMConstant getExpectedValue() {
        return expected;
    }

}
