package lama.sqlite3;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lama.DatabaseFacade;
import lama.DatabaseProvider;
import lama.IgnoreMeException;
import lama.Main.QueryManager;
import lama.Main.StateLogger;
import lama.MainOptions;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce;
import lama.StateToReproduce.SQLite3StateToReproduce;
import lama.sqlite3.dml.SQLite3DeleteGenerator;
import lama.sqlite3.dml.SQLite3InsertGenerator;
import lama.sqlite3.dml.SQLite3UpdateGenerator;
import lama.sqlite3.gen.SQLite3AnalyzeGenerator;
import lama.sqlite3.gen.SQLite3Common;
import lama.sqlite3.gen.SQLite3CreateVirtualRtreeTabelGenerator;
import lama.sqlite3.gen.SQLite3ExplainGenerator;
import lama.sqlite3.gen.SQLite3PragmaGenerator;
import lama.sqlite3.gen.SQLite3ReindexGenerator;
import lama.sqlite3.gen.SQLite3TransactionGenerator;
import lama.sqlite3.gen.SQLite3VacuumGenerator;
import lama.sqlite3.gen.SQLite3VirtualFTSTableCommandGenerator;
import lama.sqlite3.gen.ddl.SQLite3AlterTable;
import lama.sqlite3.gen.ddl.SQLite3CreateTriggerGenerator;
import lama.sqlite3.gen.ddl.SQLite3CreateVirtualFTSTableGenerator;
import lama.sqlite3.gen.ddl.SQLite3DropIndexGenerator;
import lama.sqlite3.gen.ddl.SQLite3DropTableGenerator;
import lama.sqlite3.gen.ddl.SQLite3IndexGenerator;
import lama.sqlite3.gen.ddl.SQLite3TableGenerator;
import lama.sqlite3.gen.ddl.SQLite3ViewGenerator;
import lama.sqlite3.queries.SQLite3MetamorphicQuerySynthesizer;
import lama.sqlite3.queries.SQLite3PivotedQuerySynthesizer;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;
import lama.sqlite3.schema.SQLite3Schema.Table.TableKind;

public class SQLite3Provider implements DatabaseProvider {

	public static enum Action {
		PRAGMA {
			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return SQLite3PragmaGenerator.insertPragma(con, state, r);
			}
		},
		INDEX {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return SQLite3IndexGenerator.insertIndex(newSchema, state, r);
			}
		},
		INSERT {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				Table randomTable = newSchema.getRandomTableOrBailout(t -> !t.isView());
				return SQLite3InsertGenerator.insertRow(randomTable, con, r);
			}

		},
		VACUUM {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3VacuumGenerator.executeVacuum();
			}

		},
		REINDEX {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3ReindexGenerator.executeReindex(con, state, newSchema);
			}

		},
		ANALYZE {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3AnalyzeGenerator.generateAnalyze(newSchema);

			}
		},
		DELETE {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3DeleteGenerator
						.deleteContent(newSchema.getRandomTableNoViewOrBailout(), con, r);
			}
		},
		TRANSACTION_START {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3TransactionGenerator.generateBeginTransaction(con, state);
			}

		},
		ALTER {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return SQLite3AlterTable.alterTable(newSchema, con, state, r);
			}

		},
		DROP_INDEX {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return SQLite3DropIndexGenerator.dropIndex(con, state, newSchema, r);
			}
		},
		UPDATE {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3UpdateGenerator.updateRow(newSchema.getRandomTableNoViewOrBailout(), r);
			}
		},
		ROLLBACK_TRANSACTION() {
			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3TransactionGenerator.generateRollbackTransaction(con, state);
			}
		},
		COMMIT {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3TransactionGenerator.generateCommit(con, state);
			}

		},
		DROP_TABLE {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r) {
				return SQLite3DropTableGenerator.dropTable(newSchema);
			}

		},
		DROP_VIEW {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return SQLite3ViewGenerator.dropView(SQLite3Schema.fromConnection(con));
			}

		},
		EXPLAIN {

			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return SQLite3ExplainGenerator.explain(con, (SQLite3StateToReproduce) state, r);
			}
		},
		CHECK_RTREE_TABLE {
			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				Table table = newSchema.getRandomTableOrBailout(t -> t.getName().startsWith("r"));
				return new QueryAdapter(String.format("SELECT rtreecheck('%s');", table.getName()));
			}
		},
//		TARGETED_SELECT {
//
//			@Override
//			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
//					throws SQLException {
//				return new SQLite3PivotedQuerySynthesizer(con, r).getQueryThatContainsAtLeastOneRow((SQLite3StateToReproduce) state);
//			}
//
//		},
		VIRTUAL_TABLE_ACTION {
			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return new SQLite3VirtualFTSTableCommandGenerator(newSchema, r).generate();
			}
		},
		CREATE_VIEW {
			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return SQLite3ViewGenerator.generate(newSchema, con, r, (SQLite3StateToReproduce) state);
			}
		},
		CREATE_TRIGGER {
			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				return SQLite3CreateTriggerGenerator.create(newSchema, r, con);
			}
		},
		MANIPULATE_STAT_TABLE {
			@Override
			public Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
					throws SQLException {
				List<Column> columns = new ArrayList<>();
				Table t = new Table("sqlite_stat1", columns, TableKind.MAIN, false, 1, false, false);
				if (Randomly.getBoolean()) {
					return SQLite3DeleteGenerator.deleteContent(t, con, r);
				} else {
					StringBuilder sb = new StringBuilder();
					sb.append("INSERT OR IGNORE INTO sqlite_stat1");
					String indexName;
					try (Statement stat = con.createStatement()) {
						try (ResultSet rs = stat.executeQuery(
								"SELECT name FROM sqlite_master WHERE type='index' ORDER BY RANDOM() LIMIT 1;")) {
							if (rs.isClosed()) {
								throw new IgnoreMeException();
							}
							indexName = rs.getString("name");
						};
					}
					sb.append(" VALUES");
					sb.append("('");
					sb.append(newSchema.getRandomTable().getName());
					sb.append("', ");
					sb.append("'");
					if (Randomly.getBoolean()) {
						sb.append(indexName);
					} else {
						sb.append(newSchema.getRandomTable().getName());
					}
					sb.append("'");
					sb.append(", '");
					for (int i = 0; i < Randomly.smallNumber(); i++) {
						if (i != 0) {
							sb.append(" ");
						}
						if (Randomly.getBoolean()) {
							sb.append(r.getInteger());
						} else {
							sb.append(Randomly.smallNumber());
						}
					}
					if (Randomly.getBoolean()) {
						sb.append(" sz=");
						sb.append(r.getInteger());
					}
					if (Randomly.getBoolean()) {
						sb.append(" unordered");
					}
					if (Randomly.getBoolean()) {
						sb.append(" noskipscan");
					}
					sb.append("')");
					return new QueryAdapter(sb.toString(), Arrays.asList("no such table"));
				}
			}
		};


		public abstract Query getQuery(SQLite3Schema newSchema, Connection con, StateToReproduce state, Randomly r)
				throws SQLException;
	}

	public static final int NR_INSERT_ROW_TRIES = 30;
	private static final int NR_QUERIES_PER_TABLE = 10000;
	private static final int MAX_INSERT_ROW_TRIES = 0;
	public static final int EXPRESSION_MAX_DEPTH = 3;
	public static final boolean ALLOW_FLOATING_POINT_FP = true;
	public static final boolean MUST_KNOW_RESULT = false;

	private SQLite3StateToReproduce state;
	private String databaseName;
	
	public static class SQLite3SpecialStringGenerator {
		
		private enum Options {
			TIME_DATE_REGEX, NOW, DATE_TIME, TIME_MODIFIER
		}
		
		public static String generate() {
			StringBuilder sb = new StringBuilder();
			switch (Randomly.fromOptions(Options.values())) {
			case TIME_DATE_REGEX: // https://www.sqlite.org/lang_datefunc.html
				return Randomly.fromOptions("%d", "%f", "%H", "%j", "%J", "%m", "%M", "%s", "%S", "%w", "%W", "%Y", "%%");
			case NOW:
				return "now";
			case DATE_TIME:
				long notCachedInteger = Randomly.getNotCachedInteger(1, 10);
				for (int i = 0; i < notCachedInteger; i++) {
					if (Randomly.getBoolean()) {
						sb.append(Randomly.getNonCachedInteger());
					} else {
						sb.append(Randomly.getNotCachedInteger(0, 2000));
					}
					sb.append(Randomly.fromOptions(":", "-", " ", "T"));
				}
				return sb.toString();
			case TIME_MODIFIER:
				sb.append(Randomly.fromOptions("days", "hours", "minutes", "seconds", "months", "years", "start of month", "start of year", "start of day", "weekday", "unixepoch", "utc"));
				return sb.toString();
			default:
				throw new AssertionError();
			}
			
		}
	}

	@Override
	public void generateAndTestDatabase(String databaseName, Connection con, StateLogger logger, StateToReproduce state,
			QueryManager manager, MainOptions options) throws SQLException {
		this.databaseName = databaseName;
		Randomly r = new Randomly(SQLite3SpecialStringGenerator::generate);
		SQLite3Schema newSchema = null;
		this.state = (SQLite3StateToReproduce) state;

		addSensiblePragmaDefaults(con);
		int nrTablesToCreate = 1 + Randomly.smallNumber();
		for (int i = 0; i < 1; i++) {
			newSchema = SQLite3Schema.fromConnection(con);
//			assert newSchema.getDatabaseTables().size() == i : newSchema + " " + i;
			String tableName = SQLite3Common.createTableName(i);
			Query tableQuery = SQLite3TableGenerator.createTableStatement(tableName, state, newSchema, r);
			manager.execute(tableQuery);
			if (true) {
				String ftsTableName = "v" + SQLite3Common.createTableName(i);
				Query tableQuery2 = SQLite3CreateVirtualFTSTableGenerator.createTableStatement(ftsTableName, r);
				manager.execute(tableQuery2);
			}
			if (true) {
				String rTreeTableName = "rt" + i;
				Query tableQuery3 = SQLite3CreateVirtualRtreeTabelGenerator.createTableStatement(rTreeTableName, r);
				manager.execute(tableQuery3);
			}
			
		}
		newSchema = SQLite3Schema.fromConnection(con);

		int[] nrRemaining = new int[Action.values().length];
		List<Action> actions = new ArrayList<>();
		int total = 0;
		for (int i = 0; i < Action.values().length; i++) {
			Action action = Action.values()[i];
			int nrPerformed = 0;
			switch (action) {
			case DROP_VIEW:
				nrPerformed = r.getInteger(0, 0);
				break;
			case CREATE_TRIGGER:
				nrPerformed = r.getInteger(0, 2);
				break;
			case CREATE_VIEW:
				nrPerformed = r.getInteger(0, 2);
				break;
			case ALTER:
			case EXPLAIN:
				nrPerformed = 0;
				break;
			case DROP_TABLE:
			case DELETE:
			case DROP_INDEX:
			case VACUUM:
			case CHECK_RTREE_TABLE:
				nrPerformed = r.getInteger(0, 3);
				break;
			case INSERT:
				nrPerformed = r.getInteger(1, NR_INSERT_ROW_TRIES);
				break;
			case MANIPULATE_STAT_TABLE:
				nrPerformed = r.getInteger(0, 5);
				break;
//			case TARGETED_SELECT:
//				nrPerformed = 0; // r.getInteger(0, 100);
//				break;
			case INDEX:
				nrPerformed = r.getInteger(0, 20);
				break;
			case VIRTUAL_TABLE_ACTION:
			case UPDATE:
				nrPerformed = r.getInteger(0, 30);
				break;
			case PRAGMA:
				nrPerformed = r.getInteger(0, 100);
				break;
			case COMMIT:
			case TRANSACTION_START:
			case REINDEX:
			case ANALYZE:
			case ROLLBACK_TRANSACTION:
			default:
				nrPerformed = r.getInteger(1, 10);
				break;
			}
			if (nrPerformed != 0) {
				actions.add(action);
			}
			nrRemaining[action.ordinal()] = nrPerformed;
			total += nrPerformed;
		}

		while (total != 0) {
			Action nextAction = null;
			int selection = r.getInteger(0, total);
			int previousRange = 0;
			for (int i = 0; i < nrRemaining.length; i++) {
				if (previousRange <= selection && selection < previousRange + nrRemaining[i]) {
					nextAction = Action.values()[i];
					break;
				} else {
					previousRange += nrRemaining[i];
				}
			}
			assert nextAction != null;
			assert nrRemaining[nextAction.ordinal()] > 0;
			nrRemaining[nextAction.ordinal()]--;
			Query query = nextAction.getQuery(newSchema, con, state, r);
			try {
				if (options.logEachSelect()) {
					try {
						logger.getCurrentFileWriter().close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					logger.currentFileWriter = null;
					logger.writeCurrent(state);
					logger.writeCurrent(query.getQueryString());
				}
				manager.execute(query);
				if (query.couldAffectSchema()) {
					newSchema = SQLite3Schema.fromConnection(con);
				}
			} catch (IgnoreMeException e) {

			} 
			total--;
		}
		Query query = SQLite3TransactionGenerator.generateCommit(con, state);
		manager.execute(query);
		// also do an abort for DEFERRABLE INITIALLY DEFERRED
		query = SQLite3TransactionGenerator.generateRollbackTransaction(con, state);
		manager.execute(query);
		newSchema = SQLite3Schema.fromConnection(con);

		if (Randomly.getBoolean()) {
			query = new QueryAdapter("PRAGMA integrity_check;");
			manager.execute(query);
		}
//		for (Table t : newSchema.getDatabaseTables()) {
//			if (t.getNrRows() == 0) {
//				throw new IgnoreMeException();
//			}
//		}

		newSchema = SQLite3Schema.fromConnection(con);

//		SQLite3PivotedQuerySynthesizer queryGenerator = new SQLite3PivotedQuerySynthesizer(con, r);
		SQLite3MetamorphicQuerySynthesizer or = new SQLite3MetamorphicQuerySynthesizer(newSchema, r, con,
				(SQLite3StateToReproduce) state, logger, options);
		if (options.logEachSelect()) {
			logger.writeCurrent(state);
		}
		for (int i = 0; i < NR_QUERIES_PER_TABLE; i++) {

			try {
//				if (Randomly.getBoolean()) {
				or.generateAndCheck();
				manager.incrementSelectQueryCount();
//				} else {
//					queryGenerator.generateAndCheckQuery(this.state, logger, options);
//				}
			} catch (IgnoreMeException e) {

			}
		}
		try {
			if (options.logEachSelect()) {
				logger.getCurrentFileWriter().close();
				logger.currentFileWriter = null;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.gc();
	}

	// PRAGMAS to achieve good performance
	private final static List<String> DEFAULT_PRAGMAS = Arrays.asList("PRAGMA cache_size = 50000;",
			"PRAGMA temp_store=MEMORY;", "PRAGMA synchronous=off;");

	private void addSensiblePragmaDefaults(Connection con) throws SQLException {
		List<String> pragmasToExecute = new ArrayList<>();
		if (!Randomly.getBooleanWithSmallProbability()) {
			pragmasToExecute.addAll(DEFAULT_PRAGMAS);
		}
		if (Randomly.getBoolean() && !MUST_KNOW_RESULT) {
			pragmasToExecute.add("PRAGMA case_sensitive_like=ON;");
		}
		if (Randomly.getBoolean()) {
			pragmasToExecute.add(String.format("PRAGMA encoding = '%s';", Randomly.fromOptions("UTF-8", "UTF-16", "UTF-16le", "UTF-16be")));
		}
		for (String s : pragmasToExecute) {
			Query q = new QueryAdapter(s);
			state.statements.add(q);
			q.execute(con);
		}
	}

	private boolean ensureTableHasRows(Connection con, Table randomTable, Randomly r)
			throws AssertionError, SQLException {
		int nrRows;
		int counter = MAX_INSERT_ROW_TRIES;
		do {
			try {
				Query q = SQLite3InsertGenerator.insertRow(randomTable, con, r);
				state.statements.add(q);
				q.execute(con);

			} catch (SQLException e) {
				if (!SQLite3PivotedQuerySynthesizer.shouldIgnoreException(e)) {
					throw new AssertionError(e);
				}
			}
			nrRows = randomTable.getNrRows();
		} while (nrRows == 0 && counter-- != 0);
		return nrRows != 0;
	}

	@Override
	public Connection createDatabase(String databaseName, StateToReproduce state) throws SQLException {
		return DatabaseFacade.createDatabase(databaseName);
	}

	@Override
	public String getLogFileSubdirectoryName() {
		return "sqlite3";
	}

	@Override
	public String toString() {
		return String.format("SQLite3Provider [database: %s]", databaseName);
	}

	@Override
	public void printDatabaseSpecificState(FileWriter writer, StateToReproduce state) {
		StringBuilder sb = new StringBuilder();
		SQLite3StateToReproduce specificState = (SQLite3StateToReproduce) state;
		if (specificState.getRandomRowValues() != null) {
			List<Column> columnList = specificState.getRandomRowValues().keySet().stream().collect(Collectors.toList());
			List<Table> tableList = columnList.stream().map(c -> c.getTable()).distinct().sorted()
					.collect(Collectors.toList());
			for (Table t : tableList) {
				sb.append("-- " + t.getName() + "\n");
				List<Column> columnsForTable = columnList.stream().filter(c -> c.getTable().equals(t))
						.collect(Collectors.toList());
				for (Column c : columnsForTable) {
					sb.append("--\t");
					sb.append(c);
					sb.append("=");
					sb.append(specificState.getRandomRowValues().get(c));
					sb.append("\n");
				}
			}
			sb.append("-- expected values: \n");
			String asExpectedValues = "-- "
					+ SQLite3Visitor.asExpectedValues(specificState.getWhereClause()).replace("\n", "\n-- ");
			sb.append(asExpectedValues);
		}
		try {
			writer.write(sb.toString());
			writer.flush();
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public StateToReproduce getStateToReproduce(String databaseName) {
		return new SQLite3StateToReproduce(databaseName);
	}

	@Override
	public Query checkIfRowIsStillContained(StateToReproduce state) {
		String checkRowIsInside = "SELECT " + state.queryTargetedColumnsString + " FROM "
				+ state.queryTargetedTablesString + " INTERSECT SELECT " + state.values;
		return new QueryAdapter(checkRowIsInside);
	}

}
