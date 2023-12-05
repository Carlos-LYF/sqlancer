package sqlancer.gaussdbm.gen;

import sqlancer.Randomly;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTables;
import sqlancer.gaussdbm.ast.GaussDBMConstant;
import sqlancer.gaussdbm.ast.GaussDBMExpression;
import sqlancer.gaussdbm.ast.GaussDBMSelect;
import sqlancer.gaussdbm.ast.GaussDBMTableReference;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class GaussDBMRandomQuerySynthesizer {

    private GaussDBMRandomQuerySynthesizer() {
    }

    public static GaussDBMSelect generate(GaussDBMGlobalState globalState, int nrColumns) {
        GaussDBMTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        GaussDBMExpressionGenerator gen = new GaussDBMExpressionGenerator(globalState).setColumns(tables.getColumns());
        GaussDBMSelect select = new GaussDBMSelect();

        select.setSelectType(Randomly.fromOptions(GaussDBMSelect.SelectType.values()));
        List<GaussDBMExpression> columns = new ArrayList<>(gen.generateExpressions(nrColumns));
        select.setFetchColumns(columns);
        List<GaussDBMExpression> tableList = tables.getTables().stream().map(GaussDBMTableReference::new)
                .collect(Collectors.toList());
        select.setFromList(tableList);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateHavingClause());
            }
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(GaussDBMConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(GaussDBMConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        return select;
    }

}
