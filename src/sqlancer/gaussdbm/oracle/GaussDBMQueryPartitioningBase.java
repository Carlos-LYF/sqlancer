package sqlancer.gaussdbm.oracle;

import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.gaussdbm.GaussDBMErrors;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTables;
import sqlancer.gaussdbm.ast.GaussDBMColumnReference;
import sqlancer.gaussdbm.ast.GaussDBMExpression;
import sqlancer.gaussdbm.ast.GaussDBMSelect;
import sqlancer.gaussdbm.ast.GaussDBMTableReference;
import sqlancer.gaussdbm.gen.GaussDBMExpressionGenerator;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public abstract class GaussDBMQueryPartitioningBase extends
        TernaryLogicPartitioningOracleBase<GaussDBMExpression, GaussDBMGlobalState> implements TestOracle<GaussDBMGlobalState> {

    GaussDBMSchema s;
    GaussDBMTables targetTables;
    GaussDBMExpressionGenerator gen;
    GaussDBMSelect select;

    public GaussDBMQueryPartitioningBase(GaussDBMGlobalState state) {
        super(state);
        GaussDBMErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new GaussDBMExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new GaussDBMSelect();
        select.setFetchColumns(generateFetchColumns());
        List<GaussDBMTable> tables = targetTables.getTables();
        List<GaussDBMExpression> tableList = tables.stream().map(t -> new GaussDBMTableReference(t))
                .collect(Collectors.toList());
        // List<GaussDBMExpression> joins = GaussDBMJoin.getJoins(tableList, state);
        select.setFromList(tableList);
        select.setWhereClause(null);
        // select.setJoins(joins);
    }

    List<GaussDBMExpression> generateFetchColumns() {
        return Arrays.asList(GaussDBMColumnReference.create(targetTables.getColumns().get(0), null));
    }

    @Override
    protected ExpressionGenerator<GaussDBMExpression> getGen() {
        return gen;
    }

}
