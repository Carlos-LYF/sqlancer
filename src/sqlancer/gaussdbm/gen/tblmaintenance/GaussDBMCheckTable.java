package sqlancer.gaussdbm.gen.tblmaintenance;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTable;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @see <a href="https://dev.GaussDBM.com/doc/refman/8.0/en/check-table.html">CHECK TABLE Statement</a>
 */
public class GaussDBMCheckTable {

    private final List<GaussDBMTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public GaussDBMCheckTable(List<GaussDBMTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter check(GaussDBMGlobalState globalState) {
        return new GaussDBMCheckTable(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty()).generate();
    }

    // CHECK TABLE tbl_name [, tbl_name] ... [option] ...
    //
    // option: {
    // FOR UPGRADE
    // | QUICK
    // | FAST
    // | MEDIUM
    // | EXTENDED
    // | CHANGED
    // }
    private SQLQueryAdapter generate() {
        sb.append("CHECK TABLE ");
        sb.append(tables.stream().map(AbstractTable::getName).collect(Collectors.joining(", ")));
        sb.append(" ");
        List<String> options = Randomly.subset("FOR UPGRADE", "QUICK", "FAST", "MEDIUM", "EXTENDED", "CHANGED");
        sb.append(String.join(" ", options));
        return new SQLQueryAdapter(sb.toString());
    }

}
