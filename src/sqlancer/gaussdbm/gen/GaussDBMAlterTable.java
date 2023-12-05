package sqlancer.gaussdbm.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class GaussDBMAlterTable {

    private final GaussDBMSchema schema;
    private final StringBuilder sb = new StringBuilder();
    boolean couldAffectSchema;

    public GaussDBMAlterTable(GaussDBMSchema newSchema) {
        this.schema = newSchema;
    }

    public static SQLQueryAdapter create(GaussDBMGlobalState globalState) {
        return new GaussDBMAlterTable(globalState.getSchema()).create();
    }

    private enum Action {
        ALGORITHM, //
//        CHECKSUM, //
        COMPRESSION, //
        DISABLE_ENABLE_KEYS("Data truncated for functional index"), /* ignore due to http://bugs.GaussDBM.com/?id=96295 */
        DROP_COLUMN("Cannot drop column", "ALGORITHM=INPLACE is not supported.", "ALGORITHM=INSTANT is not supported.",
                "Duplicate entry", "has a partitioning function dependency and cannot be dropped or renamed.",
                "A primary key index cannot be invisible" /*
         * this error should not occur, see
         * https://bugs.GaussDBM.com/bug.php?id=95897
         */,
                "Field in list of fields for partition function not found in table", "in 'partition function'",
                "has a functional index dependency and cannot be dropped or renamed."),
        FORCE, //
        // ORDER_BY is supported, see below
        DELAY_KEY_WRITE, //
        INSERT_METHOD, //
        ROW_FORMAT, //
        STATS_AUTO_RECALC, //
        STATS_PERSISTENT, //
        PACK_KEYS, RENAME("doesn't exist", "already exists"); /* WITH_WITHOUT_VALIDATION , */
//        DROP_PRIMARY_KEY(
//                "ALGORITHM=INSTANT is not supported. Reason: Dropping a primary key is not allowed without also adding a new primary key. Try ALGORITHM=COPY/INPLACE.");

        private final String[] potentialErrors;

        Action(String... couldCauseErrors) {
            this.potentialErrors = couldCauseErrors.clone();
        }

    }

    private SQLQueryAdapter create() {
        ExpectedErrors errors = ExpectedErrors.from("does not support the create option", "doesn't have this option",
                "is not supported for this operation", "Data truncation", "Specified key was too long");
        errors.add("Data truncated for functional index ");
        sb.append("ALTER TABLE ");
        GaussDBMTable table = schema.getRandomTable();
        sb.append(table.getName());
        sb.append(" ");
        List<Action> list = new ArrayList<>(Arrays.asList(Action.values()));
        if (table.getColumns().size() == 1) {
            list.remove(Action.DROP_COLUMN);
        }
        List<Action> selectedActions = Randomly.subset(list);
        int i = 0;
        for (Action a : selectedActions) {
            if (i++ != 0) {
                sb.append(", ");
            }
            switch (a) {
                case ALGORITHM:
                    sb.append("ALGORITHM ");
                    sb.append(Randomly.fromOptions("INSTANT", "INPLACE", "COPY", "DEFAULT"));
                    break;
//                case CHECKSUM:
//                    sb.append("CHECKSUM ");
//                    sb.append(Randomly.fromOptions(0, 1));
//                    break;
                case COMPRESSION:
                    sb.append("COMPRESSION ");
                    sb.append("'");
                    sb.append(Randomly.fromOptions("ZLIB", "LZ4", "NONE"));
                    sb.append("'");
                    break;
                case DELAY_KEY_WRITE:
                    sb.append("DELAY_KEY_WRITE ");
                    sb.append(Randomly.fromOptions(0, 1));
                    break;
                case DROP_COLUMN:
                    sb.append("DROP ");
                    if (Randomly.getBoolean()) {
                        sb.append("COLUMN ");
                    }
                    sb.append(table.getRandomColumn().getName());
                    couldAffectSchema = true;
                    break;
                case DISABLE_ENABLE_KEYS:
                    sb.append(Randomly.fromOptions("DISABLE", "ENABLE"));
                    sb.append(" KEYS");
                    break;
//                case DROP_PRIMARY_KEY:
//                    assert table.hasPrimaryKey();
//                    sb.append("DROP PRIMARY KEY");
//                    couldAffectSchema = true;
//                    break;
                case FORCE:
                    sb.append("FORCE");
                    break;
                case INSERT_METHOD:
                    sb.append("INSERT_METHOD ");
                    sb.append(Randomly.fromOptions("NO", "FIRST", "LAST"));
                    break;
                case ROW_FORMAT:
                    sb.append("ROW_FORMAT ");
                    sb.append(Randomly.fromOptions("DEFAULT", "DYNAMIC", "FIXED", "COMPRESSED", "REDUNDANT", "COMPACT"));
                    break;
                case STATS_AUTO_RECALC:
                    sb.append("STATS_AUTO_RECALC ");
                    sb.append(Randomly.fromOptions(0, 1, "DEFAULT"));
                    break;
                case STATS_PERSISTENT:
                    sb.append("STATS_PERSISTENT ");
                    sb.append(Randomly.fromOptions(0, 1, "DEFAULT"));
                    break;
                case PACK_KEYS:
                    sb.append("PACK_KEYS ");
                    sb.append(Randomly.fromOptions(0, 1, "DEFAULT"));
                    break;
                // not relevant:
                case RENAME:
                    sb.append("RENAME TO ");
                    // 改一下不支持的重命名表语法，我们只有RENAME TO，把RENAME和RENAME AS去掉
//                    if (Randomly.getBoolean()) {
//                        sb.append(Randomly.fromOptions("TO", "AS"));
//                        sb.append(" ");
//                    }
                    sb.append("t");
                    sb.append(Randomly.smallNumber());
                    couldAffectSchema = true;
                    break;
                default:
                    throw new AssertionError(a);
            }
        }
        if (Randomly.getBooleanWithSmallProbability()) {
            if (i != 0) {
                sb.append(", ");
            }
            // should be given as last option
            sb.append(" ORDER BY ");
            sb.append(table.getRandomNonEmptyColumnSubset().stream().map(AbstractTableColumn::getName)
                    .collect(Collectors.joining(", ")));
        }
        for (Action a : selectedActions) {
            for (String error : a.potentialErrors) {
                errors.add(error);
            }
        }
        return new SQLQueryAdapter(sb.toString(), errors, couldAffectSchema);
    }

}