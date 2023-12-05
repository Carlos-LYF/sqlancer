package sqlancer.gaussdbm.oracle;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.gaussdbm.GaussDBMErrors;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMColumn;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMRowValue;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTables;
import sqlancer.gaussdbm.GaussDBMVisitor;
import sqlancer.gaussdbm.ast.*;
import sqlancer.gaussdbm.ast.GaussDBMUnaryPostfixOperation.UnaryPostfixOperator;
import sqlancer.gaussdbm.ast.GaussDBMUnaryPrefixOperation.GaussDBMUnaryPrefixOperator;
import sqlancer.gaussdbm.gen.GaussDBMExpressionGenerator;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class GaussDBMPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<GaussDBMGlobalState, GaussDBMRowValue, GaussDBMExpression, SQLConnection> {

    private List<GaussDBMExpression> fetchColumns;
    private List<GaussDBMColumn> columns;

    public GaussDBMPivotedQuerySynthesisOracle(GaussDBMGlobalState globalState) throws SQLException {
        super(globalState);
        GaussDBMErrors.addExpressionErrors(errors);
        errors.add("in 'order clause'"); // e.g., Unknown column '2067708013' in 'order clause'
    }

    @Override
    public Query<SQLConnection> getRectifiedQuery() throws SQLException {
        GaussDBMTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();
        List<GaussDBMTable> tables = randomFromTables.getTables();

        GaussDBMSelect selectStatement = new GaussDBMSelect();
        selectStatement.setSelectType(Randomly.fromOptions(GaussDBMSelect.SelectType.values()));
        columns = randomFromTables.getColumns();
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

        selectStatement.setFromList(tables.stream().map(t -> new GaussDBMTableReference(t)).collect(Collectors.toList()));

        fetchColumns = columns.stream().map(c -> new GaussDBMColumnReference(c, null)).collect(Collectors.toList());
        selectStatement.setFetchColumns(fetchColumns);
        GaussDBMExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        List<GaussDBMExpression> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        GaussDBMExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            GaussDBMExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<String> modifiers = Randomly.subset("STRAIGHT_JOIN", "SQL_SMALL_RESULT", "SQL_BIG_RESULT", "SQL_NO_CACHE");
        selectStatement.setModifiers(modifiers);
        List<GaussDBMExpression> orderBy = new GaussDBMExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBys();
        selectStatement.setOrderByExpressions(orderBy);

        return new SQLQueryAdapter(GaussDBMVisitor.asString(selectStatement), errors);
    }

    private List<GaussDBMExpression> generateGroupByClause(List<GaussDBMColumn> columns, GaussDBMRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> GaussDBMColumnReference.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private GaussDBMConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return GaussDBMConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private GaussDBMExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return GaussDBMConstant.createIntConstantNotAsBoolean(0);
        } else {
            return null;
        }
    }

    private GaussDBMExpression generateRectifiedExpression(List<GaussDBMColumn> columns, GaussDBMRowValue rw) {
        GaussDBMExpression expression = new GaussDBMExpressionGenerator(globalState).setRowVal(rw).setColumns(columns)
                .generateExpression();
        GaussDBMConstant expectedValue = expression.getExpectedValue();
        GaussDBMExpression result;
        if (expectedValue.isNull()) {
            result = new GaussDBMUnaryPostfixOperation(expression, UnaryPostfixOperator.IS_NULL, false);
        } else if (expectedValue.asBooleanNotNull()) {
            result = expression;
        } else {
            result = new GaussDBMUnaryPrefixOperation(expression, GaussDBMUnaryPrefixOperator.NOT);
        }
        rectifiedPredicates.add(result);
        return result;
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> query) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
        sb.append(query.getUnterminatedQueryString());
        sb.append(") as result WHERE ");
        int i = 0;
        for (GaussDBMColumn c : columns) {
            if (i++ != 0) {
                sb.append(" AND ");
            }
            sb.append("result.");
            sb.append("ref");
            sb.append(i - 1);
            if (pivotRow.getValues().get(c).isNull()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" = ");
                sb.append(pivotRow.getValues().get(c).getTextRepresentation());
            }
        }

        String resultingQueryString = sb.toString();
        return new SQLQueryAdapter(resultingQueryString, query.getExpectedErrors());
    }

    @Override
    protected String getExpectedValues(GaussDBMExpression expr) {
        return GaussDBMVisitor.asExpectedValues(expr);
    }
}
