package sqlancer.gaussdbm;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.gaussdbm.GaussDBMOptions.GaussDBMOracleFactory;
import sqlancer.gaussdbm.oracle.GaussDBMCERTOracle;
import sqlancer.gaussdbm.oracle.GaussDBMFuzzer;
import sqlancer.gaussdbm.oracle.GaussDBMPivotedQuerySynthesisOracle;
import sqlancer.gaussdbm.oracle.GaussDBMTLPWhereOracle;

import java.sql.SQLException;
import java.util.List;

@Parameters(separators = "=", commandDescription = "GaussDBM (default port: " + GaussDBMOptions.DEFAULT_PORT
        + ", default host: " + GaussDBMOptions.DEFAULT_HOST + ")")
public class GaussDBMOptions implements DBMSSpecificOptions<GaussDBMOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    @Parameter(names = "--oracle")
    public List<GaussDBMOracleFactory> oracles = List.of(GaussDBMOracleFactory.TLP_WHERE);

    public enum GaussDBMOracleFactory implements OracleFactory<GaussDBMGlobalState> {

        TLP_WHERE {
            @Override
            public TestOracle<GaussDBMGlobalState> create(GaussDBMGlobalState globalState) throws SQLException {
                return new GaussDBMTLPWhereOracle(globalState);
            }

        },
        PQS {
            @Override
            public TestOracle<GaussDBMGlobalState> create(GaussDBMGlobalState globalState) throws SQLException {
                return new GaussDBMPivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }

        },
        CERT {
            @Override
            public TestOracle<GaussDBMGlobalState> create(GaussDBMGlobalState globalState) throws SQLException {
                return new GaussDBMCERTOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        },
        FUZZER {
            @Override
            public TestOracle<GaussDBMGlobalState> create(GaussDBMGlobalState globalState) throws Exception {
                return new GaussDBMFuzzer(globalState);
            }

        }
    }

    @Override
    public List<GaussDBMOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
