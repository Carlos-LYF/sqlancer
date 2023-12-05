package sqlancer.gaussdbm.ast;

public interface GaussDBMExpression {

    default GaussDBMConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

}
