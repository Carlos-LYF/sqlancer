package sqlancer.gaussdbm.ast;

import sqlancer.IgnoreMeException;

import java.util.List;

/**
 * @see <a href="https://dev.GaussDBM.com/doc/refman/8.0/en/comparison-operators.html#operator_in">Comparison Functions and
 * Operators</a>
 */
public class GaussDBMInOperation implements GaussDBMExpression {

    private final GaussDBMExpression expr;
    private final List<GaussDBMExpression> listElements;
    private final boolean isTrue;

    public GaussDBMInOperation(GaussDBMExpression expr, List<GaussDBMExpression> listElements, boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public GaussDBMExpression getExpr() {
        return expr;
    }

    public List<GaussDBMExpression> getListElements() {
        return listElements;
    }

    @Override
    public GaussDBMConstant getExpectedValue() {
        GaussDBMConstant leftVal = expr.getExpectedValue();
        if (leftVal.isNull()) {
            return GaussDBMConstant.createNullConstant();
        }
        /* workaround for https://bugs.GaussDBM.com/bug.php?id=95957 */
        if (leftVal.isInt() && !leftVal.isSigned()) {
            throw new IgnoreMeException();
        }

        boolean isNull = false;
        for (GaussDBMExpression rightExpr : listElements) {
            GaussDBMConstant rightVal = rightExpr.getExpectedValue();

            /* workaround for https://bugs.GaussDBM.com/bug.php?id=95957 */
            if (rightVal.isInt() && !rightVal.isSigned()) {
                throw new IgnoreMeException();
            }
            GaussDBMConstant isEquals = leftVal.isEquals(rightVal);
            if (isEquals.isNull()) {
                isNull = true;
            } else {
                if (isEquals.getInt() == 1) {
                    return GaussDBMConstant.createBoolean(isTrue);
                }
            }
        }
        if (isNull) {
            return GaussDBMConstant.createNullConstant();
        } else {
            return GaussDBMConstant.createBoolean(!isTrue);
        }

    }

    public boolean isTrue() {
        return isTrue;
    }
}
