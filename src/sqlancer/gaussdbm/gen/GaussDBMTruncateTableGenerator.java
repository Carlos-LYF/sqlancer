package sqlancer.gaussdbm.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.gaussdbm.GaussDBMGlobalState;

public final class GaussDBMTruncateTableGenerator {

    private GaussDBMTruncateTableGenerator() {
    }

    public static SQLQueryAdapter generate(GaussDBMGlobalState globalState) {
        return new SQLQueryAdapter("TRUNCATE TABLE " + globalState.getSchema().getRandomTable().getName(), ExpectedErrors.from("doesn't have this option"));
    }

}
