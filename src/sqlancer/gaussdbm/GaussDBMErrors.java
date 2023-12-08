package sqlancer.gaussdbm;

import sqlancer.common.query.ExpectedErrors;

import java.util.regex.Pattern;

public final class GaussDBMErrors {

    private GaussDBMErrors() {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.add("smallint out of range");
        errors.add("tinyint out of range");
        errors.add("BIGINT value is out of range"); // e.g., CAST(-('-1e500') AS SIGNED)
        errors.add("is not valid for CHARACTER SET");
    }

    public static void addInsertUpdateErrors(ExpectedErrors errors) {
        addExpressionErrors(errors);

        errors.add("doesn't have a default value");
        errors.add("Data truncation");
        errors.add("Incorrect integer value");
        errors.add("Duplicate entry");
        errors.add("Data truncated for column");
        errors.add("Data truncated for functional index");
        errors.add("cannot be null");
        errors.add("violates not-null constraint");
        errors.add("cannot cast type boolean to double");
        errors.add("incorrect double value");
        errors.add("Incorrect decimal value");
        errors.add("Distributed key column can't be updated in current version");
        errors.add("Distributed key column of GSI can't be updated in current version");
        errors.add("invalid input syntax for");
        errors.add("must be type boolean, not type"); // 11号版本把这个放开
        errors.add("zero-length delimited identifier at or near \"\"\"\"");
        errors.add("REPLACE INTO on GSI is not yet supported");

        errors.addRegex(Pattern.compile("function ifnull\\([a-z]+( [a-z]+)?, [a-z]+( [a-z]+)?\\) does not exist")); // 11号版本这个也放开
        errors.addRegex(Pattern.compile("NVL2 types [a-z]+( [a-z]+)? and [a-z]+( [a-z]+)? cannot be matched"));
    }

}
