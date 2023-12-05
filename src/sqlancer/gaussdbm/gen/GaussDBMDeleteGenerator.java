package sqlancer.gaussdbm.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.gaussdbm.GaussDBMErrors;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;
import sqlancer.gaussdbm.GaussDBMVisitor;

import java.util.Arrays;

public class GaussDBMDeleteGenerator {

    private final StringBuilder sb = new StringBuilder();
    private final GaussDBMGlobalState globalState;

    public GaussDBMDeleteGenerator(GaussDBMGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter delete(GaussDBMGlobalState globalState) {
        return new GaussDBMDeleteGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        GaussDBMTable randomTable = globalState.getSchema().getRandomTable();
        GaussDBMExpressionGenerator gen = new GaussDBMExpressionGenerator(globalState).setColumns(randomTable.getColumns());
        ExpectedErrors errors = new ExpectedErrors();
        sb.append("DELETE");
        if (Randomly.getBoolean()) {
            sb.append(" LOW_PRIORITY");
        }
        if (Randomly.getBoolean()) {
            sb.append(" QUICK");
        }
        if (Randomly.getBoolean()) {
            sb.append(" IGNORE");
        }
        // TODO: support partitions
        sb.append(" FROM ");
        sb.append(randomTable.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(GaussDBMVisitor.asString(gen.generateExpression()));
            GaussDBMErrors.addExpressionErrors(errors);
        }
        errors.addAll(Arrays.asList("doesn't have this option",
                "Truncated incorrect DOUBLE value" /*
                 * ignore as a workaround for https://bugs.GaussDBM.com/bug.php?id=95997
                 */, "Truncated incorrect INTEGER value",
                "Truncated incorrect DECIMAL value", "Data truncated for functional index"));
        // TODO: support ORDER BY
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
