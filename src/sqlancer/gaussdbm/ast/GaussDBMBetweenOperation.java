package sqlancer.gaussdbm.ast;

import sqlancer.IgnoreMeException;
import sqlancer.gaussdbm.ast.GaussDBMBinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.gaussdbm.ast.GaussDBMBinaryLogicalOperation.GaussDBMBinaryLogicalOperator;

public class GaussDBMBetweenOperation implements GaussDBMExpression {

    private final GaussDBMExpression expr;
    private final GaussDBMExpression left;
    private final GaussDBMExpression right;

    public GaussDBMBetweenOperation(GaussDBMExpression expr, GaussDBMExpression left, GaussDBMExpression right) {
        this.expr = expr;
        this.left = left;
        this.right = right;
    }

    public GaussDBMExpression getExpr() {
        return expr;
    }

    public GaussDBMExpression getLeft() {
        return left;
    }

    public GaussDBMExpression getRight() {
        return right;
    }

    @Override
    public GaussDBMConstant getExpectedValue() {
        GaussDBMExpression[] arr = {left, right, expr};
        GaussDBMConstant convertedExpr = GaussDBMComputableFunction.castToMostGeneralType(expr.getExpectedValue(), arr);
        GaussDBMConstant convertedLeft = GaussDBMComputableFunction.castToMostGeneralType(left.getExpectedValue(), arr);
        GaussDBMConstant convertedRight = GaussDBMComputableFunction.castToMostGeneralType(right.getExpectedValue(), arr);

        /* workaround for https://bugs.GaussDBM.com/bug.php?id=96006 */
        if (convertedLeft.isInt() && convertedLeft.getInt() < 0 || convertedRight.isInt() && convertedRight.getInt() < 0
                || convertedExpr.isInt() && convertedExpr.getInt() < 0) {
            throw new IgnoreMeException();
        }
        GaussDBMBinaryLogicalOperation andOperation = getGaussDBMBinaryLogicalOperation(convertedLeft, convertedExpr, convertedRight);
        return andOperation.getExpectedValue();
    }

    private static GaussDBMBinaryLogicalOperation getGaussDBMBinaryLogicalOperation(GaussDBMConstant convertedLeft, GaussDBMConstant convertedExpr, GaussDBMConstant convertedRight) {
        GaussDBMBinaryComparisonOperation leftComparison = new GaussDBMBinaryComparisonOperation(convertedLeft, convertedExpr,
                BinaryComparisonOperator.LESS_EQUALS);
        GaussDBMBinaryComparisonOperation rightComparison = new GaussDBMBinaryComparisonOperation(convertedExpr,
                convertedRight, BinaryComparisonOperator.LESS_EQUALS);
        return new GaussDBMBinaryLogicalOperation(leftComparison, rightComparison,
                GaussDBMBinaryLogicalOperator.AND);
    }

}
