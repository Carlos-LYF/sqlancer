package sqlancer.gaussdbm.gen.tblmaintenance;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTable;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @see <a href="https://dev.GaussDBM.com/doc/refman/8.0/en/checksum-table.html">CHECKSUM TABLE Statement</a>
 */
public class GaussDBMChecksum {

    private final List<GaussDBMTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public GaussDBMChecksum(List<GaussDBMTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter checksum(GaussDBMGlobalState globalState) {
        return new GaussDBMChecksum(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty()).checksum();
    }

    // CHECKSUM TABLE tbl_name [, tbl_name] ... [QUICK | EXTENDED]
    private SQLQueryAdapter checksum() {
        sb.append("CHECKSUM TABLE ");
        sb.append(tables.stream().map(AbstractTable::getName).collect(Collectors.joining(", ")));
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("QUICK", "EXTENDED"));
        }
        return new SQLQueryAdapter(sb.toString());
    }

}
