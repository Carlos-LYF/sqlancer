package sqlancer.gaussdbm;

import sqlancer.Randomly;
import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.gaussdbm.ast.*;
import sqlancer.gaussdbm.ast.GaussDBMOrderByTerm.GaussDBMOrder;

import java.util.List;
import java.util.stream.Collectors;

public class GaussDBMToStringVisitor extends ToStringVisitor<GaussDBMExpression> implements GaussDBMVisitor {

    int ref;

    @Override
    public void visitSpecific(GaussDBMExpression expr) {
        GaussDBMVisitor.super.visit(expr);
    }

    @Override
    public void visit(GaussDBMSelect s) {
        sb.append("SELECT ");
        switch (s.getFromOptions()) {
            case DISTINCT:
                sb.append("DISTINCT ");
                break;
            case ALL:
                sb.append(Randomly.fromOptions("ALL ", ""));
                break;
            case DISTINCTROW:
                sb.append("DISTINCTROW ");
                break;
            default:
                throw new AssertionError();
        }
        sb.append(String.join(" ", s.getModifiers()));
        if (!s.getModifiers().isEmpty()) {
            sb.append(" ");
        }
        if (s.getFetchColumns() == null) {
            sb.append("*");
        } else {
            for (int i = 0; i < s.getFetchColumns().size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(s.getFetchColumns().get(i));
                // GaussDBM does not allow duplicate column names
                sb.append(" AS ");
                sb.append("ref");
                sb.append(ref++);
            }
        }
        sb.append(" FROM ");
        for (int i = 0; i < s.getFromList().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(s.getFromList().get(i));
        }
        for (GaussDBMExpression j : s.getJoinList()) {
            visit(j);
        }

        if (s.getWhereClause() != null) {
            GaussDBMExpression whereClause = s.getWhereClause();
            sb.append(" WHERE ");
            visit(whereClause);
        }
        if (s.getGroupByExpressions() != null && !s.getGroupByExpressions().isEmpty()) {
            sb.append(" ");
            sb.append("GROUP BY ");
            List<GaussDBMExpression> groupBys = s.getGroupByExpressions();
            for (int i = 0; i < groupBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(groupBys.get(i));
            }
        }
        if (!s.getOrderByExpressions().isEmpty()) {
            sb.append(" ORDER BY ");
            List<GaussDBMExpression> orderBys = s.getOrderByExpressions();
            for (int i = 0; i < orderBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(s.getOrderByExpressions().get(i));
            }
        }
        if (s.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(s.getLimitClause());
        }

        if (s.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(s.getOffsetClause());
        }
    }

    @Override
    public void visit(GaussDBMConstant constant) {
        sb.append(constant.getTextRepresentation());
    }

    @Override
    public String get() {
        return super.get();
    }

    @Override
    public void visit(GaussDBMColumnReference column) {
        sb.append(column.getColumn().getFullQualifiedName());
    }

    @Override
    public void visit(GaussDBMUnaryPostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" IS ");
        if (op.isNegated()) {
            sb.append("NOT ");
        }
        switch (op.getOperator()) {
            case IS_FALSE:
                sb.append("FALSE");
                break;
            case IS_NULL:
                if (Randomly.getBoolean()) {
                    sb.append("UNKNOWN");
                } else {
                    sb.append("NULL");
                }
                break;
            case IS_TRUE:
                sb.append("TRUE");
                break;
            default:
                throw new AssertionError(op);
        }
    }

    @Override
    public void visit(GaussDBMComputableFunction f) {
        sb.append(f.getFunction().getName());
        sb.append("(");
        for (int i = 0; i < f.getArguments().length; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(f.getArguments()[i]);
        }
        sb.append(")");
    }

    @Override
    public void visit(GaussDBMBinaryLogicalOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getTextRepresentation());
        sb.append(" ");
        sb.append("(");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(GaussDBMBinaryComparisonOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(GaussDBMCastOperation op) {
        sb.append("CAST(");
        visit(op.getExpr());
        sb.append(" AS ");
        sb.append(op.getType());
        sb.append(")");
    }

    @Override
    public void visit(GaussDBMInOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(")");
        if (!op.isTrue()) {
            sb.append(" NOT");
        }
        sb.append(" IN ");
        sb.append("(");
        for (int i = 0; i < op.getListElements().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(op.getListElements().get(i));
        }
        sb.append(")");
    }

    @Override
    public void visit(GaussDBMBinaryOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(GaussDBMOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder() == GaussDBMOrder.ASC ? "ASC" : "DESC");
    }

    @Override
    public void visit(GaussDBMExists op) {
        sb.append(" EXISTS (");
        visit(op.getExpr());
        sb.append(")");
    }

    @Override
    public void visit(GaussDBMStringExpression op) {
        sb.append(op.getStr());
    }

    @Override
    public void visit(GaussDBMBetweenOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(") BETWEEN (");
        visit(op.getLeft());
        sb.append(") AND (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(GaussDBMTableReference ref) {
        sb.append(ref.getTable().getName());
    }

    @Override
    public void visit(GaussDBMCollate collate) {
        sb.append("(");
        visit(collate.getExpression());
        sb.append(" ");
        sb.append(collate.getOperatorRepresentation());
        sb.append(")");
    }

}
