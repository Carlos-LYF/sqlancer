package sqlancer.gaussdbm.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.gaussdbm.GaussDBMErrors;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMColumn;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;
import sqlancer.gaussdbm.GaussDBMVisitor;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class GaussDBMInsertGenerator {

    private final GaussDBMTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final GaussDBMGlobalState globalState;

    public GaussDBMInsertGenerator(GaussDBMGlobalState globalState, GaussDBMTable table) {
        this.globalState = globalState;
        this.table = table;
    }

    public static SQLQueryAdapter insertRow(GaussDBMGlobalState globalState) throws SQLException {
        GaussDBMTable table = globalState.getSchema().getRandomTable();
        return insertRow(globalState, table);
    }

    public static SQLQueryAdapter insertRow(GaussDBMGlobalState globalState, GaussDBMTable table) throws SQLException {
        if (Randomly.getBoolean()) {
            return new GaussDBMInsertGenerator(globalState, table).generateInsert();
        } else {
            return new GaussDBMInsertGenerator(globalState, table).generateReplace();
        }
    }

    private SQLQueryAdapter generateReplace() {
        sb.append("REPLACE");
        // 不支持相关语法
//        if (Randomly.getBoolean()) {
//            sb.append(" ");
//            sb.append(Randomly.fromOptions("LOW_PRIORITY", "DELAYED"));
//        }
        return generateInto();

    }

    private SQLQueryAdapter generateInsert() {
        sb.append("INSERT");
//        if (Randomly.getBoolean()) {
//            sb.append(" ");
//            sb.append(Randomly.fromOptions("LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY"));
//        }
        if (Randomly.getBoolean()) {
            sb.append(" IGNORE");
        }
        return generateInto();
    }

    private SQLQueryAdapter generateInto() {
        sb.append(" INTO ");
        sb.append(table.getName());
        List<GaussDBMColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append("(");
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(") ");
        sb.append("VALUES");
        GaussDBMExpressionGenerator gen = new GaussDBMExpressionGenerator(globalState);
        int nrRows;
        if (Randomly.getBoolean()) {
            nrRows = 1;
        } else {
            nrRows = 1 + Randomly.smallNumber();
        }
        for (int row = 0; row < nrRows; row++) {
            if (row != 0) {
                sb.append(", ");
            }
            sb.append("(");
            for (int c = 0; c < columns.size(); c++) {
                if (c != 0) {
                    sb.append(", ");
                }
                sb.append(GaussDBMVisitor.asString(gen.generateConstant()));

            }
            sb.append(")");
        }
        GaussDBMErrors.addInsertUpdateErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
