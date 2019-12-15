package lama.postgres.gen;

import java.util.ArrayList;
import java.util.List;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresSchema;

public class PostgresSequenceGenerator {

	public static Query createSequence(Randomly r, PostgresSchema s) {
		List<String> errors = new ArrayList<>();
		StringBuilder sb = new StringBuilder("CREATE");
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("TEMPORARY", "TEMP"));
		}
		sb.append(" SEQUENCE");
		// TODO keep track of sequences
		sb.append(" IF NOT EXISTS");
		// TODO generate sequence names
		sb.append(" seq");
		if (Randomly.getBoolean()) {
			sb.append(" AS ");
			sb.append(Randomly.fromOptions("smallint", "integer", "bigint"));
		}
		if (Randomly.getBoolean()) {
			sb.append(" INCREMENT");
			if (Randomly.getBoolean()) {
				sb.append(" BY");
			}
			sb.append(" ");
			sb.append(r.getInteger());
			errors.add("INCREMENT must not be zero");
		}
		if (Randomly.getBoolean()) {
			if (Randomly.getBoolean()) {
				sb.append(" MINVALUE");
				sb.append(" ");
				sb.append(r.getInteger());
			} else {
				sb.append(" NO MINVALUE");
			}
			errors.add("must be less than MAXVALUE");
		}
		if (Randomly.getBoolean()) {
			if (Randomly.getBoolean()) {
				sb.append(" MAXVALUE");
				sb.append(" ");
				sb.append(r.getInteger());
			} else {
				sb.append(" NO MAXVALUE");
			}
			errors.add("must be less than MAXVALUE");
		}
		if (Randomly.getBoolean()) {
			sb.append(" START");
			if (Randomly.getBoolean()) {
				sb.append(" WITH");
			}
			sb.append(" ");
			sb.append(r.getInteger());
			errors.add("cannot be less than MINVALUE");
			errors.add("cannot be greater than MAXVALUE");
		}
		if (Randomly.getBoolean()) {
			sb.append(" CACHE ");
			sb.append(r.getPositiveIntegerNotNull());
		}
		errors.add("is out of range");
		if (Randomly.getBoolean()) {
			if (Randomly.getBoolean()) {
				sb.append(" NO");
			}
			sb.append(" CYCLE");
		}
		if (Randomly.getBoolean()) {
			sb.append(" OWNED BY ");
//			if (Randomly.getBoolean()) {
			sb.append("NONE");
//			} else {
//				sb.append(s.getRandomTable().getRandomColumn().getFullQualifiedName());
//			}
		}
		return new QueryAdapter(sb.toString(), errors);
	}

}
