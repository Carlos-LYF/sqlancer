package sqlancer.gaussdbm.gen.admin;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTable;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;

import java.util.List;
import java.util.stream.Collectors;

/*
 * https://dev.mysql.com/doc/refman/8.0/en/flush.html#flush-tables-variants
 */
public class GaussDBMFlush {

    private final List<GaussDBMTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public GaussDBMFlush(List<GaussDBMTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter create(GaussDBMGlobalState globalState) {
        return new GaussDBMFlush(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty()).generate();
    }

    private SQLQueryAdapter generate() {
        sb.append("FLUSH");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
            sb.append(" ");
            // TODO: | RELAY LOGS [FOR CHANNEL channel] not fully implemented
            List<String> options = Randomly.nonEmptySubset("BINARY LOGS", "ENGINE LOGS", "ERROR LOGS", "GENERAL LOGS",
                    "HOSTS", "LOGS", "PRIVILEGES", "OPTIMIZER_COSTS", "RELAY LOGS", "SLOW LOGS", "STATUS",
                    "USER_RESOURCES");
            sb.append(String.join(", ", options));
        } else {
            sb.append(" ");
            sb.append("TABLES");
            if (Randomly.getBoolean()) {
                sb.append(" ");
                sb.append(tables.stream().map(AbstractTable::getName).collect(Collectors.joining(", ")));
                // TODO implement READ LOCK and other variants
            }
        }
        return new SQLQueryAdapter(sb.toString());
    }

}
