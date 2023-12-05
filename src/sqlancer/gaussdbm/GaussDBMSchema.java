package sqlancer.gaussdbm;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.*;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable.GaussDBMEngine;
import sqlancer.gaussdbm.ast.GaussDBMConstant;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class GaussDBMSchema extends AbstractSchema<GaussDBMGlobalState, GaussDBMTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    public enum GaussDBMDataType {
        INT, VARCHAR, FLOAT, DOUBLE, DECIMAL;

        public static GaussDBMDataType getRandom(GaussDBMGlobalState globalState) {
            if (globalState.usesPQS()) {
                return Randomly.fromOptions(GaussDBMDataType.INT, GaussDBMDataType.VARCHAR);
            } else {
                return Randomly.fromOptions(values());
            }
        }

        public boolean isNumeric() {
            switch (this) {
                case INT:
                case DOUBLE:
                case FLOAT:
                case DECIMAL:
                    return true;
                case VARCHAR:
                    return false;
                default:
                    throw new AssertionError(this);
            }
        }

    }

    public static class GaussDBMColumn extends AbstractTableColumn<GaussDBMTable, GaussDBMDataType> {

        private final boolean isPrimaryKey;
        private final int precision;

        public GaussDBMColumn(String name, GaussDBMDataType columnType, boolean isPrimaryKey, int precision) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.precision = precision;
        }

        public int getPrecision() {
            return precision;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

    }

    public static class GaussDBMTables extends AbstractTables<GaussDBMTable, GaussDBMColumn> {

        public GaussDBMTables(List<GaussDBMTable> tables) {
            super(tables);
        }

        public GaussDBMRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String randomRow = String.format("SELECT %s FROM %s ORDER BY RAND() LIMIT 1", columnNamesAsString(
                            c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    // columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." +
                    // c.getName() + ")")
                    tableNamesAsString());
            Map<GaussDBMColumn, GaussDBMConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    GaussDBMColumn column = getColumns().get(i);
                    Object value;
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    GaussDBMConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = GaussDBMConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                            case INT:
                                value = randomRowValues.getLong(columnIndex);
                                constant = GaussDBMConstant.createIntConstant((long) value);
                                break;
                            case VARCHAR:
                                value = randomRowValues.getString(columnIndex);
                                constant = GaussDBMConstant.createStringConstant((String) value);
                                break;
                            default:
                                throw new AssertionError(column.getType());
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new GaussDBMRowValue(this, values);
            }

        }

    }

    private static GaussDBMDataType getColumnType(String typeString) {
        switch (typeString) {
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "int":
            case "bigint":
                return GaussDBMDataType.INT;
            case "varchar":
            case "tinytext":
            case "mediumtext":
            case "text":
            case "longtext":
                return GaussDBMDataType.VARCHAR;
            case "double":
                return GaussDBMDataType.DOUBLE;
            case "float":
                return GaussDBMDataType.FLOAT;
            case "decimal":
                return GaussDBMDataType.DECIMAL;
            default:
                throw new AssertionError(typeString);
        }
    }

    public static class GaussDBMRowValue extends AbstractRowValue<GaussDBMTables, GaussDBMColumn, GaussDBMConstant> {

        GaussDBMRowValue(GaussDBMTables tables, Map<GaussDBMColumn, GaussDBMConstant> values) {
            super(tables, values);
        }

    }

    public static class GaussDBMTable extends AbstractRelationalTable<GaussDBMColumn, GaussDBMIndex, GaussDBMGlobalState> {

        public enum GaussDBMEngine {
            INNO_DB("InnoDB"), MY_ISAM("MyISAM"), MEMORY("MEMORY"), HEAP("HEAP"), CSV("CSV"), MERGE("MERGE"),
            ARCHIVE("ARCHIVE"), FEDERATED("FEDERATED");

            private final String s;

            GaussDBMEngine(String s) {
                this.s = s;
            }

            public static GaussDBMEngine get(String val) {
                return Stream.of(values()).filter(engine -> engine.s.equalsIgnoreCase(val)).findFirst().get();
            }

        }

        private final GaussDBMEngine engine;

        public GaussDBMTable(String tableName, List<GaussDBMColumn> columns, List<GaussDBMIndex> indexes, GaussDBMEngine engine) {
            super(tableName, columns, indexes, false /* TODO: support views */);
            this.engine = engine;
        }

        public GaussDBMEngine getEngine() {
            return engine;
        }

        public boolean hasPrimaryKey() {
            return getColumns().stream().anyMatch(GaussDBMColumn::isPrimaryKey);
        }

    }

    public static final class GaussDBMIndex extends TableIndex {

        private GaussDBMIndex(String indexName) {
            super(indexName);
        }

        public static GaussDBMIndex create(String indexName) {
            return new GaussDBMIndex(indexName);
        }

        @Override
        public String getIndexName() {
            if (super.getIndexName().contentEquals("PRIMARY")) {
                return "`PRIMARY`";
            } else {
                return super.getIndexName();
            }
        }

    }

    public static GaussDBMSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        /* the loop is a workaround for https://bugs.GaussDBM.com/bug.php?id=95929 */
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<GaussDBMTable> databaseTables = new ArrayList<>();
                try (Statement s = con.createStatement()) {
                    try (ResultSet rs = s.executeQuery(
                            "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '"
                                    + databaseName + "';")) {
                        while (rs.next()) {
                            String tableName = rs.getString("TABLE_NAME");
                            String tableEngineStr = rs.getString("ENGINE");
                            GaussDBMEngine engine = GaussDBMEngine.get(tableEngineStr);
                            List<GaussDBMColumn> databaseColumns = getTableColumns(con, tableName, databaseName);
                            List<GaussDBMIndex> indexes = getIndexes(con, tableName, databaseName);
                            GaussDBMTable t = new GaussDBMTable(tableName, databaseColumns, indexes, engine);
                            for (GaussDBMColumn c : databaseColumns) {
                                c.setTable(t);
                            }
                            databaseTables.add(t);
                        }
                    }
                }
                return new GaussDBMSchema(databaseTables);
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    private static List<GaussDBMIndex> getIndexes(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<GaussDBMIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s';",
                    databaseName, tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    indexes.add(GaussDBMIndex.create(indexName));
                }
            }
        }
        return indexes;
    }

    private static List<GaussDBMColumn> getTableColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<GaussDBMColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from information_schema.columns where table_schema = '"
                    + databaseName + "' AND TABLE_NAME='" + tableName + "'")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    int precision = rs.getInt("NUMERIC_PRECISION");
                    boolean isPrimaryKey = rs.getString("COLUMN_KEY").equals("PRI");
                    GaussDBMColumn c = new GaussDBMColumn(columnName, getColumnType(dataType), isPrimaryKey, precision);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    public GaussDBMSchema(List<GaussDBMTable> databaseTables) {
        super(databaseTables);
    }

    public GaussDBMTables getRandomTableNonEmptyTables() {
        return new GaussDBMTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

}
