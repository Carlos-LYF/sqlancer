package sqlancer.gaussdbm.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.gaussdbm.GaussDBMErrors;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMColumn;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;
import sqlancer.gaussdbm.GaussDBMVisitor;

import java.sql.SQLException;
import java.util.List;

public class GaussDBMUpdateGenerator extends AbstractUpdateGenerator<GaussDBMColumn> {

    private final GaussDBMGlobalState globalState;
    private GaussDBMExpressionGenerator gen;

    public GaussDBMUpdateGenerator(GaussDBMGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter create(GaussDBMGlobalState globalState) throws SQLException {
        return new GaussDBMUpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        GaussDBMTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<GaussDBMColumn> columns = table.getRandomNonEmptyColumnSubset();
        gen = new GaussDBMExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        updateColumns(columns);
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            GaussDBMErrors.addExpressionErrors(errors);
            sb.append(GaussDBMVisitor.asString(gen.generateExpression()));
        }
        GaussDBMErrors.addInsertUpdateErrors(errors);
        errors.add("doesn't have this option");

        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void updateValue(GaussDBMColumn column) {
        if (Randomly.getBoolean()) {
            sb.append(gen.generateConstant());
        } else if (Randomly.getBoolean()) {
            sb.append("DEFAULT");
        } else {
            sb.append(GaussDBMVisitor.asString(gen.generateExpression()));
        }
    }

}
