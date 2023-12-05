package sqlancer.gaussdbm.ast;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.gaussdbm.ast.GaussDBMCastOperation.CastType;

import java.util.function.BinaryOperator;

public class GaussDBMBinaryOperation implements GaussDBMExpression {

    private final GaussDBMExpression left;
    private final GaussDBMExpression right;
    private final GaussDBMBinaryOperator op;

    public enum GaussDBMBinaryOperator {

        AND("&") {
            @Override
            public GaussDBMConstant apply(GaussDBMConstant left, GaussDBMConstant right) {
                return applyBitOperation(left, right, (l, r) -> l & r);
            }

        }, OR("|") {
            @Override
            public GaussDBMConstant apply(GaussDBMConstant left, GaussDBMConstant right) {
                return applyBitOperation(left, right, (l, r) -> l | r);
            }
        }, XOR("^") {
            @Override
            public GaussDBMConstant apply(GaussDBMConstant left, GaussDBMConstant right) {
                return applyBitOperation(left, right, (l, r) -> l ^ r);
            }
        };

        private final String textRepresentation;

        private static GaussDBMConstant applyBitOperation(GaussDBMConstant left, GaussDBMConstant right, BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return GaussDBMConstant.createNullConstant();
            } else {
                long leftVal = left.castAs(CastType.SIGNED).getInt();
                long rightVal = right.castAs(CastType.SIGNED).getInt();
                long value = op.apply(leftVal, rightVal);
                return GaussDBMConstant.createUnsignedIntConstant(value);
            }
        }

        GaussDBMBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract GaussDBMConstant apply(GaussDBMConstant left, GaussDBMConstant right);

        public static GaussDBMBinaryOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public GaussDBMBinaryOperation(GaussDBMExpression left, GaussDBMExpression right, GaussDBMBinaryOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public GaussDBMConstant getExpectedValue() {
        GaussDBMConstant leftExpected = left.getExpectedValue();
        GaussDBMConstant rightExpected = right.getExpectedValue();

        /* workaround for https://bugs.GaussDBM.com/bug.php?id=95960 */
        processExpected(leftExpected);

        processExpected(rightExpected);

        return op.apply(leftExpected, rightExpected);
    }

    private void processExpected(GaussDBMConstant expected) {
        if (expected.isString()) {
            String text = expected.castAsString();
            while (text.startsWith(" ") || text.startsWith("\t")) {
                text = text.substring(1);
            }
            if (!text.isEmpty() && (text.startsWith("\n") || text.startsWith("."))) {
                throw new IgnoreMeException();
            }
        }
    }

    public GaussDBMExpression getLeft() {
        return left;
    }

    public GaussDBMBinaryOperator getOp() {
        return op;
    }

    public GaussDBMExpression getRight() {
        return right;
    }

}
