package sqlancer.gaussdbm.gen.datadef;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.gaussdbm.GaussDBMErrors;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMColumn;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMDataType;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;
import sqlancer.gaussdbm.GaussDBMVisitor;
import sqlancer.gaussdbm.ast.GaussDBMExpression;
import sqlancer.gaussdbm.gen.GaussDBMExpressionGenerator;

import java.util.List;

public class GaussDBMIndexGenerator {

    private final Randomly r;
    private StringBuilder sb = new StringBuilder();
    private boolean containsInPlace;
    private final GaussDBMSchema schema;
    private final GaussDBMGlobalState globalState;

    public GaussDBMIndexGenerator(GaussDBMSchema schema, Randomly r, GaussDBMGlobalState globalState) {
        this.schema = schema;
        this.r = r;
        this.globalState = globalState;
    }

    public static SQLQueryAdapter create(GaussDBMGlobalState globalState) {
        return new GaussDBMIndexGenerator(globalState.getSchema(), globalState.getRandomly(), globalState).create();
    }

    public SQLQueryAdapter create() {
        ExpectedErrors errors = new ExpectedErrors();
        GaussDBMErrors.addExpressionErrors(errors);
        sb.append("CREATE ");
        if (Randomly.getBoolean()) {
            // "FULLTEXT" TODO Column 'c3' cannot be part of FULLTEXT index
            // A SPATIAL index may only contain a geometrical type column
            sb.append("UNIQUE ");
            errors.add("Duplicate entry");
        }
        sb.append("INDEX ");
        sb.append(globalState.getSchema().getFreeIndexName());
        indexType();
        sb.append(" ON ");
        GaussDBMTable table = schema.getRandomTable();
        GaussDBMExpressionGenerator gen = new GaussDBMExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append(table.getName());
        sb.append("(");
        if (Randomly.getBoolean()) {
            for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append("(");
                GaussDBMExpression randExpr = gen.generateExpression();
                sb.append(GaussDBMVisitor.asString(randExpr));
                sb.append(")");

            }
        } else {
            List<GaussDBMColumn> randomColumn = table.getRandomNonEmptyColumnSubset();
            int i = 0;
            for (GaussDBMColumn c : randomColumn) {
                if (i++ != 0) {
                    sb.append(", ");
                }
                sb.append(c.getName());
                if (Randomly.getBoolean() && c.getType() == GaussDBMDataType.VARCHAR) {
                    sb.append("(");
                    // TODO for string
                    sb.append(r.getInteger(1, 5));
                    sb.append(")");
                }
                if (Randomly.getBoolean()) {
                    sb.append(" ");
                    sb.append(Randomly.fromOptions("ASC", "DESC"));
                }
            }
        }
        sb.append(")");
        algorithmOption();
        String string = sb.toString();
        sb = new StringBuilder();
        if (containsInPlace) {
            errors.add("ALGORITHM=INPLACE is not supported");
        }
        errors.add("Cannot create index whose evaluation cannot be enforced to remote nodes");
        errors.add("A primary key index cannot be invisible");
        errors.add("Functional index on a column is not supported. Consider using a regular index instead.");
        errors.add("Incorrect usage of spatial/fulltext/hash index and explicit index order");
        errors.add("The storage engine for the table doesn't support descending indexes");
        errors.add("must include all columns");
        errors.add("cannot index the expression");
        errors.add("Data truncation: Truncated incorrect");
        errors.add("a disallowed function.");
        errors.add("Data truncation");
        errors.add("Cannot create a functional index on an expression that returns a BLOB or TEXT.");
        errors.add("used in key specification without a key length");
        errors.add("can't be used in key specification with the used table type");
        errors.add("Specified key was too long");
        errors.add("out of range");
        errors.add("Data truncated for functional index");
        errors.add("used in key specification without a key length");
        errors.add("Row size too large"); // seems to happen together with MIN_ROWS in the table declaration
        return new SQLQueryAdapter(string, errors, true);
    }

    private void algorithmOption() {
        if (Randomly.getBoolean()) {
            sb.append(" ALGORITHM");
            if (Randomly.getBoolean()) {
                sb.append("=");
            }
            sb.append(" ");
            String fromOptions = Randomly.fromOptions("DEFAULT", "INPLACE", "COPY");
            if (fromOptions.contentEquals("INPLACE")) {
                containsInPlace = true;
            }
            sb.append(fromOptions);
        }
    }

    private void indexType() {
        if (Randomly.getBoolean()) {
            sb.append(" USING ");
            // USING HASH不支持，会报错
            sb.append("BTREE");
        }
    }

}
