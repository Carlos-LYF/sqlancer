package lama.postgres.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresProvider;
import lama.postgres.PostgresSchema.PostgresTable;

public class PostgresVacuumGenerator {

	public static Query create(PostgresTable table) {
		StringBuilder sb = new StringBuilder("VACUUM ");
		if (Randomly.getBoolean()) {
			// VACUUM [ ( { FULL | FREEZE | VERBOSE | ANALYZE | DISABLE_PAGE_SKIPPING } [,
			// ...] ) ] [ table_name [ (column_name [, ...] ) ] ]
			sb.append("(");
			for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
				ArrayList<String> opts = new ArrayList<String>(
						Arrays.asList("FULL", "FREEZE", "ANALYZE", "VERBOSE", "DISABLE_PAGE_SKIPPING"));
				if (PostgresProvider.IS_POSTGRES_TWELVE) {
					opts.add("SKIP_LOCKED");
					opts.add("INDEX_CLEANUP");
					opts.add("TRUNCATE");
				}
				String option = Randomly.fromList(opts);
				if (i != 0) {
					sb.append(", ");
				}
				sb.append(option);
				if (PostgresProvider.IS_POSTGRES_TWELVE && Randomly.getBoolean()) {
					sb.append(" ");
					sb.append(Randomly.fromOptions(1, 0));
				}
			}
			sb.append(")");
			if (Randomly.getBoolean()) {
				addTableAndColumns(table, sb);
			}
		} else {
			String firstOption = Randomly.fromOptions("FULL", "FREEZE", "VERBOSE");
			sb.append(firstOption);
			if (Randomly.getBoolean()) {
				sb.append(" ANALYZE");
				addTableAndColumns(table, sb);
			} else {
				if (Randomly.getBoolean()) {
					sb.append(" ");
					sb.append(table.getName());
				}
			}
		}
		List<String> errors = new ArrayList<>();
		errors.add("VACUUM cannot run inside a transaction block");
		errors.add("deadlock");  /* "FULL" commented out due to
		 * https://www.postgresql.org/message-id/CA%2Bu7OA6pL%
		 * 2B7Xm_NXHLenxffe3tCr3gTamVdr7zPjcWqW0RFM-A%40mail.gmail.com*/
		errors.add("ERROR: ANALYZE option must be specified when a column list is provided");
		errors.add("VACUUM option DISABLE_PAGE_SKIPPING cannot be used with FULL");
		return new QueryAdapter(sb.toString(), errors);
	}

	private static void addTableAndColumns(PostgresTable table, StringBuilder sb) {
		sb.append(" ");
		sb.append(table.getName());
		if (Randomly.getBoolean()) {
			sb.append("(");
			sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
					.collect(Collectors.joining(", ")));
			sb.append(")");
		}
	}

}
