package sqlancer.gaussdbm.ast;

import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMTable;

public class GaussDBMTableReference implements GaussDBMExpression {

    private final GaussDBMTable table;

    public GaussDBMTableReference(GaussDBMTable table) {
        this.table = table;
    }

    public GaussDBMTable getTable() {
        return table;
    }

}
