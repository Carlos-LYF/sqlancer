
package sqlancer.gaussdbm;

import sqlancer.SQLGlobalState;
import sqlancer.gaussdbm.GaussDBMOptions.GaussDBMOracleFactory;

import java.sql.SQLException;

public class GaussDBMGlobalState extends SQLGlobalState<GaussDBMOptions, GaussDBMSchema> {

    @Override
    protected GaussDBMSchema readSchema() throws SQLException {
        return GaussDBMSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == GaussDBMOracleFactory.PQS);
    }

}
