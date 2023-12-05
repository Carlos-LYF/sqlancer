package sqlancer.gaussdbm.ast;

import sqlancer.Randomly;

public class GaussDBMBinaryLogicalOperation implements GaussDBMExpression {

    private final GaussDBMExpression left;
    private final GaussDBMExpression right;
    private final GaussDBMBinaryLogicalOperator op;
    private final String textRepresentation;

    public enum GaussDBMBinaryLogicalOperator {
        AND("AND", "&&") {
            @Override
            public GaussDBMConstant apply(GaussDBMConstant left, GaussDBMConstant right) {
                if (left.isNull() && right.isNull()) {
                    return GaussDBMConstant.createNullConstant();
                } else if (left.isNull()) {
                    if (right.asBooleanNotNull()) {
                        return GaussDBMConstant.createNullConstant();
                    } else {
                        return GaussDBMConstant.createFalse();
                    }
                } else if (right.isNull()) {
                    if (left.asBooleanNotNull()) {
                        return GaussDBMConstant.createNullConstant();
                    } else {
                        return GaussDBMConstant.createFalse();
                    }
                } else {
                    return GaussDBMConstant.createBoolean(left.asBooleanNotNull() && right.asBooleanNotNull());
                }
            }
        },
        OR("OR", "||") {
            @Override
            public GaussDBMConstant apply(GaussDBMConstant left, GaussDBMConstant right) {
                if (!left.isNull() && left.asBooleanNotNull()) {
                    return GaussDBMConstant.createTrue();
                } else if (!right.isNull() && right.asBooleanNotNull()) {
                    return GaussDBMConstant.createTrue();
                } else if (left.isNull() || right.isNull()) {
                    return GaussDBMConstant.createNullConstant();
                } else {
                    return GaussDBMConstant.createFalse();
                }
            }
        },
        XOR("XOR") {
            @Override
            public GaussDBMConstant apply(GaussDBMConstant left, GaussDBMConstant right) {
                if (left.isNull() || right.isNull()) {
                    return GaussDBMConstant.createNullConstant();
                }
                boolean xorVal = left.asBooleanNotNull() ^ right.asBooleanNotNull();
                return GaussDBMConstant.createBoolean(xorVal);
            }
        };

        private final String[] textRepresentations;

        GaussDBMBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public abstract GaussDBMConstant apply(GaussDBMConstant left, GaussDBMConstant right);

        public static GaussDBMBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public GaussDBMBinaryLogicalOperation(GaussDBMExpression left, GaussDBMExpression right, GaussDBMBinaryLogicalOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
        this.textRepresentation = op.getTextRepresentation();
    }

    public GaussDBMExpression getLeft() {
        return left;
    }

    public GaussDBMBinaryLogicalOperator getOp() {
        return op;
    }

    public GaussDBMExpression getRight() {
        return right;
    }

    public String getTextRepresentation() {
        return textRepresentation;
    }

    @Override
    public GaussDBMConstant getExpectedValue() {
        GaussDBMConstant leftExpected = left.getExpectedValue();
        GaussDBMConstant rightExpected = right.getExpectedValue();
        if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
            return null;
        }
        return op.apply(leftExpected, rightExpected);
    }

}
