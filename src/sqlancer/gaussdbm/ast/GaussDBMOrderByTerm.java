package sqlancer.gaussdbm.ast;

import sqlancer.Randomly;

public class GaussDBMOrderByTerm implements GaussDBMExpression {

    private final GaussDBMOrder order;
    private final GaussDBMExpression expr;

    public enum GaussDBMOrder {
        ASC, DESC;

        public static GaussDBMOrder getRandomOrder() {
            return Randomly.fromOptions(GaussDBMOrder.values());
        }
    }

    public GaussDBMOrderByTerm(GaussDBMExpression expr, GaussDBMOrder order) {
        this.expr = expr;
        this.order = order;
    }

    public GaussDBMOrder getOrder() {
        return order;
    }

    public GaussDBMExpression getExpr() {
        return expr;
    }

    @Override
    public GaussDBMConstant getExpectedValue() {
        throw new AssertionError(this);
    }

}
