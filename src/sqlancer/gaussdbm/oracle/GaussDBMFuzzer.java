package sqlancer.gaussdbm.oracle;

import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMVisitor;
import sqlancer.gaussdbm.gen.GaussDBMRandomQuerySynthesizer;

public class GaussDBMFuzzer implements TestOracle<GaussDBMGlobalState> {

    private final GaussDBMGlobalState globalState;

    public GaussDBMFuzzer(GaussDBMGlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public void check() throws Exception {
        String s = GaussDBMVisitor.asString(GaussDBMRandomQuerySynthesizer.generate(globalState, Randomly.smallNumber() + 1))
                + ';';
        try {
            globalState.executeStatement(new SQLQueryAdapter(s));
            globalState.getManager().incrementSelectQueryCount();
        } catch (Error e) {

        }
    }

}
