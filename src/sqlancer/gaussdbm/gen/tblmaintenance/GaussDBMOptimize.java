package sqlancer.gaussdbm.gen.tblmaintenance;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTable;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @see <a href="https://dev.GaussDBM.com/doc/refman/8.0/en/optimize-table.html">OPTIMIZE TABLE Statement</a>
 */
public class GaussDBMOptimize {

    private final List<GaussDBMTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public GaussDBMOptimize(List<GaussDBMTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter optimize(GaussDBMGlobalState globalState) {
        return new GaussDBMOptimize(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty()).optimize();
    }

    // OPTIMIZE [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name [, tbl_name] ...
    private SQLQueryAdapter optimize() {
        sb.append("OPTIMIZE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
        }
        sb.append(" TABLE ");
        sb.append(tables.stream().map(AbstractTable::getName).collect(Collectors.joining(", ")));
        return new SQLQueryAdapter(sb.toString());
    }

}
