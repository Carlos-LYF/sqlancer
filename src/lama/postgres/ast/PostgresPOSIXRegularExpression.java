package lama.postgres.ast;

import lama.Randomly;
import lama.postgres.PostgresSchema.PostgresDataType;

public class PostgresPOSIXRegularExpression extends PostgresExpression {
	
	private PostgresExpression string;
	private PostgresExpression regex;
	private POSIXRegex op;

	public static enum POSIXRegex {
		MATCH_CASE_SENSITIVE("~"),
		MATCH_CASE_INSENSITIVE("~*"),
		NOT_MATCH_CASE_SENSITIVE("!~"),
		NOT_MATCH_CASE_INSENSITIVE("!~*");
		
		private String repr;

		private POSIXRegex(String repr) {
			this.repr = repr;
		}
		
		public String getStringRepresentation() {
			return repr;
		}
		
		public static POSIXRegex getRandom() {
			return Randomly.fromOptions(values());
		}
	}

	public PostgresPOSIXRegularExpression(PostgresExpression string, PostgresExpression regex, POSIXRegex op) {
		this.string = string;
		this.regex = regex;
		this.op = op;
	}
	
	@Override
	public PostgresDataType getExpressionType() {
		return PostgresDataType.BOOLEAN;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		return null;
	}
	
	public PostgresExpression getRegex() {
		return regex;
	}
	
	public PostgresExpression getString() {
		return string;
	}
	
	public POSIXRegex getOp() {
		return op;
	}

}
