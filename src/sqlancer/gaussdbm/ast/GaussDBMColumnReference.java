package sqlancer.gaussdbm.ast;

import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMColumn;

public class GaussDBMColumnReference implements GaussDBMExpression {

    private final GaussDBMColumn column;
    private final GaussDBMConstant value;

    public GaussDBMColumnReference(GaussDBMColumn column, GaussDBMConstant value) {
        this.column = column;
        this.value = value;
    }

    public static GaussDBMColumnReference create(GaussDBMColumn column, GaussDBMConstant value) {
        return new GaussDBMColumnReference(column, value);
    }

    public GaussDBMColumn getColumn() {
        return column;
    }

    public GaussDBMConstant getValue() {
        return value;
    }

    @Override
    public GaussDBMConstant getExpectedValue() {
        return value;
    }

}
