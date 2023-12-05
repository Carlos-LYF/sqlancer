package sqlancer.gaussdbm;

import sqlancer.gaussdbm.ast.*;

public interface GaussDBMVisitor {

    void visit(GaussDBMTableReference ref);

    void visit(GaussDBMConstant constant);

    void visit(GaussDBMColumnReference column);

    void visit(GaussDBMUnaryPostfixOperation column);

    void visit(GaussDBMComputableFunction f);

    void visit(GaussDBMBinaryLogicalOperation op);

    void visit(GaussDBMSelect select);

    void visit(GaussDBMBinaryComparisonOperation op);

    void visit(GaussDBMCastOperation op);

    void visit(GaussDBMInOperation op);

    void visit(GaussDBMBinaryOperation op);

    void visit(GaussDBMOrderByTerm op);

    void visit(GaussDBMExists op);

    void visit(GaussDBMStringExpression op);

    void visit(GaussDBMBetweenOperation op);

    void visit(GaussDBMCollate collate);

    default void visit(GaussDBMExpression expr) {
        if (expr instanceof GaussDBMConstant) {
            visit((GaussDBMConstant) expr);
        } else if (expr instanceof GaussDBMColumnReference) {
            visit((GaussDBMColumnReference) expr);
        } else if (expr instanceof GaussDBMUnaryPostfixOperation) {
            visit((GaussDBMUnaryPostfixOperation) expr);
        } else if (expr instanceof GaussDBMComputableFunction) {
            visit((GaussDBMComputableFunction) expr);
        } else if (expr instanceof GaussDBMBinaryLogicalOperation) {
            visit((GaussDBMBinaryLogicalOperation) expr);
        } else if (expr instanceof GaussDBMSelect) {
            visit((GaussDBMSelect) expr);
        } else if (expr instanceof GaussDBMBinaryComparisonOperation) {
            visit((GaussDBMBinaryComparisonOperation) expr);
        } else if (expr instanceof GaussDBMCastOperation) {
            visit((GaussDBMCastOperation) expr);
        } else if (expr instanceof GaussDBMInOperation) {
            visit((GaussDBMInOperation) expr);
        } else if (expr instanceof GaussDBMBinaryOperation) {
            visit((GaussDBMBinaryOperation) expr);
        } else if (expr instanceof GaussDBMOrderByTerm) {
            visit((GaussDBMOrderByTerm) expr);
        } else if (expr instanceof GaussDBMExists) {
            visit((GaussDBMExists) expr);
        } else if (expr instanceof GaussDBMStringExpression) {
            visit((GaussDBMStringExpression) expr);
        } else if (expr instanceof GaussDBMBetweenOperation) {
            visit((GaussDBMBetweenOperation) expr);
        } else if (expr instanceof GaussDBMTableReference) {
            visit((GaussDBMTableReference) expr);
        } else if (expr instanceof GaussDBMCollate) {
            visit((GaussDBMCollate) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(GaussDBMExpression expr) {
        GaussDBMToStringVisitor visitor = new GaussDBMToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(GaussDBMExpression expr) {
        GaussDBMExpectedValueVisitor visitor = new GaussDBMExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
