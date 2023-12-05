package sqlancer.gaussdbm;

import sqlancer.common.query.ExpectedErrors;

public final class GaussDBMErrors {

    private GaussDBMErrors() {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.add("BIGINT value is out of range"); // e.g., CAST(-('-1e500') AS SIGNED)
        errors.add("is not valid for CHARACTER SET");
    }

    public static void addInsertUpdateErrors(ExpectedErrors errors) {
        errors.add("doesn't have a default value");
        errors.add("Data truncation");
        errors.add("Incorrect integer value");
        errors.add("Duplicate entry");
        errors.add("Data truncated for column");
        errors.add("Data truncated for functional index");
        errors.add("cannot be null");
        errors.add("Incorrect decimal value");
    }

}
