package sqlancer.gaussdbm.ast;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.UnaryOperatorNode;
import sqlancer.gaussdbm.ast.GaussDBMUnaryPrefixOperation.GaussDBMUnaryPrefixOperator;

public class GaussDBMUnaryPrefixOperation extends UnaryOperatorNode<GaussDBMExpression, GaussDBMUnaryPrefixOperator>
        implements GaussDBMExpression {

    public enum GaussDBMUnaryPrefixOperator implements Operator {
        NOT("!", "NOT") {
            @Override
            public GaussDBMConstant applyNotNull(GaussDBMConstant expr) {
                return GaussDBMConstant.createIntConstant(expr.asBooleanNotNull() ? 0 : 1);
            }
        },
        PLUS("+") {
            @Override
            public GaussDBMConstant applyNotNull(GaussDBMConstant expr) {
                return expr;
            }
        },
        MINUS("-") {
            @Override
            public GaussDBMConstant applyNotNull(GaussDBMConstant expr) {
                if (expr.isString()) {
                    // TODO: implement floating points
                    throw new IgnoreMeException();
                } else if (expr.isInt()) {
                    if (!expr.isSigned()) {
                        // TODO
                        throw new IgnoreMeException();
                    }
                    return GaussDBMConstant.createIntConstant(-expr.getInt());
                } else {
                    throw new AssertionError(expr);
                }
            }
        };

        private final String[] textRepresentations;

        GaussDBMUnaryPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract GaussDBMConstant applyNotNull(GaussDBMConstant expr);

        public static GaussDBMUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public GaussDBMUnaryPrefixOperation(GaussDBMExpression expr, GaussDBMUnaryPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public GaussDBMConstant getExpectedValue() {
        GaussDBMConstant subExprVal = expr.getExpectedValue();
        if (subExprVal.isNull()) {
            return GaussDBMConstant.createNullConstant();
        } else {
            return op.applyNotNull(subExprVal);
        }
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

}
