package sqlancer.gaussdbm;

import com.google.auto.service.AutoService;
import sqlancer.*;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.gaussdbm.gen.*;
import sqlancer.gaussdbm.gen.admin.GaussDBMFlush;
import sqlancer.gaussdbm.gen.admin.GaussDBMReset;
import sqlancer.gaussdbm.gen.datadef.GaussDBMIndexGenerator;
import sqlancer.gaussdbm.gen.tblmaintenance.*;
import sqlancer.gaussdbm.GaussDBMOptions.GaussDBMOracleFactory;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMColumn;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

@AutoService(DatabaseProvider.class)
public class GaussDBMProvider extends SQLProviderAdapter<GaussDBMGlobalState, GaussDBMOptions> {

    public GaussDBMProvider() {
        super(GaussDBMGlobalState.class, GaussDBMOptions.class);
    }

    enum Action implements AbstractAction<GaussDBMGlobalState> {
        SHOW_TABLES((g) -> new SQLQueryAdapter("SHOW TABLES")), //
        INSERT(GaussDBMInsertGenerator::insertRow), //
        SET_VARIABLE(GaussDBMSetGenerator::set), //
        REPAIR(GaussDBMRepair::repair), //
        OPTIMIZE(GaussDBMOptimize::optimize), //
        CHECKSUM(GaussDBMChecksum::checksum), //
        CHECK_TABLE(GaussDBMCheckTable::check), //
        ANALYZE_TABLE(GaussDBMAnalyzeTable::analyze), //
        FLUSH(GaussDBMFlush::create), RESET(GaussDBMReset::create), CREATE_INDEX(GaussDBMIndexGenerator::create), //
        ALTER_TABLE(GaussDBMAlterTable::create), //
        TRUNCATE_TABLE(GaussDBMTruncateTableGenerator::generate), //
        SELECT_INFO((g) -> new SQLQueryAdapter(
                "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '" + g.getDatabaseName()
                        + "'")), //
        CREATE_TABLE((g) -> {
            // TODO refactor
            String tableName = DBMSCommon.createTableName(g.getSchema().getDatabaseTables().size());
            return GaussDBMTableGenerator.generate(g, tableName);
        }), //
        UPDATE(GaussDBMUpdateGenerator::create), //
        DELETE(GaussDBMDeleteGenerator::delete), //
        DROP_INDEX(GaussDBMDropIndex::generate);

        private final SQLQueryProvider<GaussDBMGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<GaussDBMGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(GaussDBMGlobalState globalState) throws Exception {
            return sqlQueryProvider.getQuery(globalState);
        }
    }

    private static int mapActions(GaussDBMGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed = 0;
        switch (a) {
        case DROP_INDEX:
            nrPerformed = r.getInteger(0, 2);
            break;
        case SHOW_TABLES:
            nrPerformed = r.getInteger(0, 1);
            break;
        case CREATE_TABLE:
            nrPerformed = r.getInteger(0, 1);
            break;
        case INSERT:
            nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            break;
        case REPAIR:
            nrPerformed = r.getInteger(0, 1);
            break;
        case SET_VARIABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case CREATE_INDEX:
            nrPerformed = r.getInteger(0, 5);
            break;
        case FLUSH:
            nrPerformed = Randomly.getBooleanWithSmallProbability() ? r.getInteger(0, 1) : 0;
            break;
        case OPTIMIZE:
            // seems to yield low CPU utilization
            nrPerformed = Randomly.getBooleanWithSmallProbability() ? r.getInteger(0, 1) : 0;
            break;
        case RESET:
            // affects the global state, so do not execute
            nrPerformed = globalState.getOptions().getNumberConcurrentThreads() == 1 ? r.getInteger(0, 1) : 0;
            break;
        case CHECKSUM:
        case CHECK_TABLE:
        case ANALYZE_TABLE:
            nrPerformed = r.getInteger(0, 2);
            break;
        case ALTER_TABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case TRUNCATE_TABLE:
            nrPerformed = r.getInteger(0, 2);
            break;
        case SELECT_INFO:
            nrPerformed = r.getInteger(0, 10);
            break;
        case UPDATE:
            nrPerformed = r.getInteger(0, 10);
            break;
        case DELETE:
            nrPerformed = r.getInteger(0, 10);
            break;
        default:
            throw new AssertionError(a);
        }
        return nrPerformed;
    }

    @Override
    public void generateDatabase(GaussDBMGlobalState globalState) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < Randomly.smallNumber() + 1) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
            SQLQueryAdapter createTable = GaussDBMTableGenerator.generate(globalState, tableName);
            globalState.executeStatement(createTable);
        }

        StatementExecutor<GaussDBMGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                GaussDBMProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();

        if (globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch((o) -> o == GaussDBMOracleFactory.CERT)) {
            // Enfore statistic collected for all tables
            ExpectedErrors errors = new ExpectedErrors();
            GaussDBMErrors.addExpressionErrors(errors);
            for (GaussDBMTable table : globalState.getSchema().getDatabaseTables()) {
                StringBuilder sb = new StringBuilder();
                sb.append("ANALYZE TABLE ");
                sb.append(table.getName());
                sb.append(" UPDATE HISTOGRAM ON ");
                String columns = table.getColumns().stream().map(GaussDBMColumn::getName)
                        .collect(Collectors.joining(", "));
                sb.append(columns).append(";");
                globalState.executeStatement(new SQLQueryAdapter(sb.toString(), errors));
            }
        }
    }

    @Override
    public SQLConnection createDatabase(GaussDBMGlobalState globalState) throws SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = GaussDBMOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = GaussDBMOptions.DEFAULT_PORT;
        }
        String databaseName = globalState.getDatabaseName();
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        globalState.getState().logStatement("USE " + databaseName);
        // 连接串格式：postgresql://10.247.42.235:6000/test
        String url = String.format("jdbc:postgresql://%s:%d/mysql_db",
                host, port);
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName + " CASCADE");
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + databaseName);
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "gaussdbm";
    }

    @Override
    public boolean addRowsToAllTables(GaussDBMGlobalState globalState) throws Exception {
        List<GaussDBMTable> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (GaussDBMTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = GaussDBMInsertGenerator.insertRow(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }

}
