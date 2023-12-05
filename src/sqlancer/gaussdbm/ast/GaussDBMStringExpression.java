package sqlancer.gaussdbm.ast;

public class GaussDBMStringExpression implements GaussDBMExpression {

    private final String str;
    private final GaussDBMConstant expectedValue;

    public GaussDBMStringExpression(String str, GaussDBMConstant expectedValue) {
        this.str = str;
        this.expectedValue = expectedValue;
    }

    public String getStr() {
        return str;
    }

    @Override
    public GaussDBMConstant getExpectedValue() {
        return expectedValue;
    }

}
