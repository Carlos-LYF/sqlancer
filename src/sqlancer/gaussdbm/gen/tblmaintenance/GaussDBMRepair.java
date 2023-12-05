package sqlancer.gaussdbm.gen.tblmaintenance;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTable;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable.GaussDBMEngine;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @see <a href="https://dev.GaussDBM.com/doc/refman/8.0/en/repair-table.html">REPAIR TABLE Statement</a>
 */
public class GaussDBMRepair {

    private final List<GaussDBMTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public GaussDBMRepair(List<GaussDBMTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter repair(GaussDBMGlobalState globalState) {
        List<GaussDBMTable> tables = globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty();
        for (GaussDBMTable table : tables) {
            // see https://bugs.GaussDBM.com/bug.php?id=95820
            if (table.getEngine() == GaussDBMEngine.MY_ISAM) {
                return new SQLQueryAdapter("SELECT 1");
            }
        }
        return new GaussDBMRepair(tables).repair();
    }

    // REPAIR [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name [, tbl_name] ...
    // [QUICK] [EXTENDED] [USE_FRM]
    private SQLQueryAdapter repair() {
        sb.append("REPAIR");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
        }
        sb.append(" TABLE ");
        sb.append(tables.stream().map(AbstractTable::getName).collect(Collectors.joining(", ")));
        if (Randomly.getBoolean()) {
            sb.append(" QUICK");
        }
        if (Randomly.getBoolean()) {
            sb.append(" EXTENDED");
        }
        if (Randomly.getBoolean()) {
            sb.append(" USE_FRM");
        }
        return new SQLQueryAdapter(sb.toString());
    }

}
