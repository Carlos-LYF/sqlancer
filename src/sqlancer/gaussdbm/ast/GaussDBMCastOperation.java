package sqlancer.gaussdbm.ast;

public class GaussDBMCastOperation implements GaussDBMExpression {

    private final GaussDBMExpression expr;
    private final CastType type;

    public enum CastType {
        SIGNED, UNSIGNED;

        public static CastType getRandom() {
            return SIGNED;
            // return Randomly.fromOptions(CastType.values());
        }

    }

    public GaussDBMCastOperation(GaussDBMExpression expr, CastType type) {
        this.expr = expr;
        this.type = type;
    }

    public GaussDBMExpression getExpr() {
        return expr;
    }

    public CastType getType() {
        return type;
    }

    @Override
    public GaussDBMConstant getExpectedValue() {
        return expr.getExpectedValue().castAs(type);
    }

}
