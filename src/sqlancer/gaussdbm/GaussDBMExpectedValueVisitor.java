package sqlancer.gaussdbm;

import sqlancer.IgnoreMeException;
import sqlancer.gaussdbm.ast.*;

public class GaussDBMExpectedValueVisitor implements GaussDBMVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(GaussDBMExpression expr) {
        GaussDBMToStringVisitor v = new GaussDBMToStringVisitor();
        v.visit(expr);
        sb.append("\t".repeat(Math.max(0, nrTabs)));
        sb.append(v.get());
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append("\n");
    }

    @Override
    public void visit(GaussDBMExpression expr) {
        nrTabs++;
        try {
            GaussDBMVisitor.super.visit(expr);
        } catch (IgnoreMeException ignored) {

        }
        nrTabs--;
    }

    @Override
    public void visit(GaussDBMConstant constant) {
        print(constant);
    }

    @Override
    public void visit(GaussDBMColumnReference column) {
        print(column);
    }

    @Override
    public void visit(GaussDBMUnaryPostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(GaussDBMComputableFunction f) {
        print(f);
        for (GaussDBMExpression expr : f.getArguments()) {
            visit(expr);
        }
    }

    @Override
    public void visit(GaussDBMBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(GaussDBMSelect select) {
        for (GaussDBMExpression j : select.getJoinList()) {
            visit(j);
        }
        if (select.getWhereClause() != null) {
            visit(select.getWhereClause());
        }
    }

    @Override
    public void visit(GaussDBMBinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(GaussDBMCastOperation op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(GaussDBMInOperation op) {
        print(op);
        for (GaussDBMExpression right : op.getListElements()) {
            visit(right);
        }
    }

    @Override
    public void visit(GaussDBMBinaryOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(GaussDBMOrderByTerm op) {

    }

    @Override
    public void visit(GaussDBMExists op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(GaussDBMStringExpression op) {
        print(op);
    }

    @Override
    public void visit(GaussDBMBetweenOperation op) {
        print(op);
        visit(op.getExpr());
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(GaussDBMTableReference ref) {
    }

    @Override
    public void visit(GaussDBMCollate collate) {
        print(collate);
        visit(collate.getExpectedValue());
    }

}
