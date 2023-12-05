package sqlancer.gaussdbm.gen.admin;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.gaussdbm.GaussDBMGlobalState;

public final class GaussDBMReset {

    private GaussDBMReset() {
    }

    public static SQLQueryAdapter create(GaussDBMGlobalState ignoredGlobalState) {
        String sb = "RESET " +
                String.join(", ", Randomly.nonEmptySubset("MASTER", "SLAVE"));
        return new SQLQueryAdapter(sb);
    }

}
