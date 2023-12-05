package sqlancer.gaussdbm.ast;

import sqlancer.common.ast.UnaryNode;

public class GaussDBMCollate extends UnaryNode<GaussDBMExpression> implements GaussDBMExpression {

    private final String collate;

    public GaussDBMCollate(GaussDBMExpression expr, String text) {
        super(expr);
        this.collate = text;
    }

    @Override
    public String getOperatorRepresentation() {
        return String.format("COLLATE '%s'", collate);
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

}
