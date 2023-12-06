package sqlancer.gaussdbm.gen.tblmaintenance;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMColumn;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @see <a href="https://dev.GaussDBM.com/doc/refman/8.0/en/analyze-table.html">ANALYZE TABLE Statement</a>
 */
public class GaussDBMAnalyzeTable {

    private final List<GaussDBMTable> tables;
    private final StringBuilder sb = new StringBuilder();
    private final Randomly r;

    public GaussDBMAnalyzeTable(List<GaussDBMTable> tables, Randomly r) {
        this.tables = tables;
        this.r = r;
    }

    public static SQLQueryAdapter analyze(GaussDBMGlobalState globalState) {
        return new GaussDBMAnalyzeTable(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty(),
                globalState.getRandomly()).generate();
    }

    private SQLQueryAdapter generate() {
        sb.append("ANALYZE ");
        if (Randomly.getBoolean()) {
            sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
        }
//        sb.append(" TABLE ");
        if (Randomly.getBoolean()) {
            analyzeWithoutHistogram();
        } else {
            if (Randomly.getBoolean()) {
                dropHistogram();
            } else {
                updateHistogram();
            }
        }
        return new SQLQueryAdapter(sb.toString());
    }

    // ANALYZE [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name [, tbl_name] ...
    private void analyzeWithoutHistogram() {
        sb.append(tables.stream().map(AbstractTable::getName).collect(Collectors.joining(", ")));
    }

    // ANALYZE [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name
    // UPDATE HISTOGRAM ON col_name [, col_name] ...
    // [WITH N BUCKETS]
    private void updateHistogram() {
        GaussDBMTable table = Randomly.fromList(tables);
        sb.append(table.getName());
        sb.append(" UPDATE HISTOGRAM ON ");
        List<GaussDBMColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        if (Randomly.getBoolean()) {
            sb.append(" WITH ");
            sb.append(r.getInteger(1, 1024));
            sb.append(" BUCKETS");
        }
    }

    // ANALYZE [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name
    // DROP HISTOGRAM ON col_name [, col_name] ...
    private void dropHistogram() {
        GaussDBMTable table = Randomly.fromList(tables);
        sb.append(table.getName());
        sb.append(" DROP HISTOGRAM ON ");
        List<GaussDBMColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
    }

}
