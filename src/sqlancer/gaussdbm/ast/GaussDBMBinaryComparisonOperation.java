package sqlancer.gaussdbm.ast;

import sqlancer.LikeImplementationHelper;
import sqlancer.Randomly;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMDataType;
import sqlancer.gaussdbm.ast.GaussDBMUnaryPrefixOperation.GaussDBMUnaryPrefixOperator;

public class GaussDBMBinaryComparisonOperation implements GaussDBMExpression {

    public enum BinaryComparisonOperator {
        EQUALS("=") {
            @Override
            public GaussDBMConstant getExpectedValue(GaussDBMConstant leftVal, GaussDBMConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public GaussDBMConstant getExpectedValue(GaussDBMConstant leftVal, GaussDBMConstant rightVal) {
                GaussDBMConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.getType() == GaussDBMDataType.INT) {
                    return GaussDBMConstant.createIntConstant(1 - isEquals.getInt());
                }
                return isEquals;
            }
        },
        LESS("<") {
            @Override
            public GaussDBMConstant getExpectedValue(GaussDBMConstant leftVal, GaussDBMConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }
        },
        LESS_EQUALS("<=") {
            @Override
            public GaussDBMConstant getExpectedValue(GaussDBMConstant leftVal, GaussDBMConstant rightVal) {
                GaussDBMConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan == null) {
                    return null;
                }
                if (lessThan.getType() == GaussDBMDataType.INT && lessThan.getInt() == 0) {
                    return leftVal.isEquals(rightVal);
                } else {
                    return lessThan;
                }
            }
        },
        GREATER(">") {
            @Override
            public GaussDBMConstant getExpectedValue(GaussDBMConstant leftVal, GaussDBMConstant rightVal) {
                GaussDBMConstant equals = leftVal.isEquals(rightVal);
                if (equals.getType() == GaussDBMDataType.INT && equals.getInt() == 1) {
                    return GaussDBMConstant.createFalse();
                } else {
                    GaussDBMConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return GaussDBMConstant.createNullConstant();
                    }
                    return GaussDBMUnaryPrefixOperator.NOT.applyNotNull(applyLess);
                }
            }
        },
        GREATER_EQUALS(">=") {
            @Override
            public GaussDBMConstant getExpectedValue(GaussDBMConstant leftVal, GaussDBMConstant rightVal) {
                GaussDBMConstant equals = leftVal.isEquals(rightVal);
                if (equals.getType() == GaussDBMDataType.INT && equals.getInt() == 1) {
                    return GaussDBMConstant.createTrue();
                } else {
                    GaussDBMConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return GaussDBMConstant.createNullConstant();
                    }
                    return GaussDBMUnaryPrefixOperator.NOT.applyNotNull(applyLess);
                }
            }

        },
        LIKE("LIKE") {
            @Override
            public GaussDBMConstant getExpectedValue(GaussDBMConstant leftVal, GaussDBMConstant rightVal) {
                if (leftVal.isNull() || rightVal.isNull()) {
                    return GaussDBMConstant.createNullConstant();
                }
                String leftStr = leftVal.castAsString();
                String rightStr = rightVal.castAsString();
                boolean matches = LikeImplementationHelper.match(leftStr, rightStr, 0, 0, false);
                return GaussDBMConstant.createBoolean(matches);
            }

        };
        // https://bugs.GaussDBM.com/bug.php?id=95908
        /*
         * IS_EQUALS_NULL_SAFE("<=>") {
         *
         * @Override public GaussDBMConstant getExpectedValue(GaussDBMConstant leftVal, GaussDBMConstant rightVal) { return
         * leftVal.isEqualsNullSafe(rightVal); }
         *
         * };
         */

        private final String textRepresentation;

        public String getTextRepresentation() {
            return textRepresentation;
        }

        BinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public abstract GaussDBMConstant getExpectedValue(GaussDBMConstant leftVal, GaussDBMConstant rightVal);

        public static BinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(BinaryComparisonOperator.values());
        }
    }

    private final GaussDBMExpression left;
    private final GaussDBMExpression right;
    private final BinaryComparisonOperator op;

    public GaussDBMBinaryComparisonOperation(GaussDBMExpression left, GaussDBMExpression right, BinaryComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public GaussDBMExpression getLeft() {
        return left;
    }

    public BinaryComparisonOperator getOp() {
        return op;
    }

    public GaussDBMExpression getRight() {
        return right;
    }

    @Override
    public GaussDBMConstant getExpectedValue() {
        return op.getExpectedValue(left.getExpectedValue(), right.getExpectedValue());
    }

}
