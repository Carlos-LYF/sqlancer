package sqlancer.gaussdbm.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.gaussdbm.GaussDBMGlobalState;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMColumn;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMRowValue;
import sqlancer.gaussdbm.ast.*;
import sqlancer.gaussdbm.ast.GaussDBMBinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.gaussdbm.ast.GaussDBMBinaryLogicalOperation.GaussDBMBinaryLogicalOperator;
import sqlancer.gaussdbm.ast.GaussDBMBinaryOperation.GaussDBMBinaryOperator;
import sqlancer.gaussdbm.ast.GaussDBMComputableFunction.GaussDBMFunction;
import sqlancer.gaussdbm.ast.GaussDBMConstant.GaussDBMDoubleConstant;
import sqlancer.gaussdbm.ast.GaussDBMOrderByTerm.GaussDBMOrder;
import sqlancer.gaussdbm.ast.GaussDBMUnaryPrefixOperation.GaussDBMUnaryPrefixOperator;

import java.util.ArrayList;
import java.util.List;

public class GaussDBMExpressionGenerator extends UntypedExpressionGenerator<GaussDBMExpression, GaussDBMColumn> {

    private final GaussDBMGlobalState state;
    private GaussDBMRowValue rowVal;

    public GaussDBMExpressionGenerator(GaussDBMGlobalState state) {
        this.state = state;
    }

    public GaussDBMExpressionGenerator setRowVal(GaussDBMRowValue rowVal) {
        this.rowVal = rowVal;
        return this;
    }

    private enum Actions {
        COLUMN, LITERAL, UNARY_PREFIX_OPERATION, UNARY_POSTFIX, COMPUTABLE_FUNCTION, BINARY_LOGICAL_OPERATOR,
        BINARY_COMPARISON_OPERATION, CAST, IN_OPERATION, BINARY_OPERATION, EXISTS, BETWEEN_OPERATOR;
    }

    @Override
    public GaussDBMExpression generateExpression(int depth) {
        if (depth >= state.getOptions().getMaxExpressionDepth()) {
            return generateLeafNode();
        }
        switch (Randomly.fromOptions(Actions.values())) {
            case COLUMN:
                return generateColumn();
            case LITERAL:
                return generateConstant();
            case UNARY_PREFIX_OPERATION:
                GaussDBMExpression subExpr = generateExpression(depth + 1);
                GaussDBMUnaryPrefixOperator random = GaussDBMUnaryPrefixOperator.getRandom();
                return new GaussDBMUnaryPrefixOperation(subExpr, random);
            case UNARY_POSTFIX:
                return new GaussDBMUnaryPostfixOperation(generateExpression(depth + 1),
                        Randomly.fromOptions(GaussDBMUnaryPostfixOperation.UnaryPostfixOperator.values()),
                        Randomly.getBoolean());
            case COMPUTABLE_FUNCTION:
                return getComputableFunction(depth + 1);
            case BINARY_LOGICAL_OPERATOR:
                return new GaussDBMBinaryLogicalOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                        GaussDBMBinaryLogicalOperator.getRandom());
            case BINARY_COMPARISON_OPERATION:
                return new GaussDBMBinaryComparisonOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                        BinaryComparisonOperator.getRandom());
            case CAST:
                return new GaussDBMCastOperation(generateExpression(depth + 1), GaussDBMCastOperation.CastType.getRandom());
            case IN_OPERATION:
                GaussDBMExpression expr = generateExpression(depth + 1);
                List<GaussDBMExpression> rightList = new ArrayList<>();
                for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
                    rightList.add(generateExpression(depth + 1));
                }
                return new GaussDBMInOperation(expr, rightList, Randomly.getBoolean());
            case BINARY_OPERATION:
                return new GaussDBMBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                        GaussDBMBinaryOperator.getRandom());
            case EXISTS:
                return getExists();
            case BETWEEN_OPERATOR:
                return new GaussDBMBetweenOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                        generateExpression(depth + 1));
            default:
                throw new AssertionError();
        }
    }

    private GaussDBMExpression getExists() {
        if (Randomly.getBoolean()) {
            return new GaussDBMExists(new GaussDBMStringExpression("SELECT 1", GaussDBMConstant.createTrue()));
        } else {
            return new GaussDBMExists(new GaussDBMStringExpression("SELECT 1 wHERE FALSE", GaussDBMConstant.createFalse()));
        }
    }

    private GaussDBMExpression getComputableFunction(int depth) {
        GaussDBMFunction func = GaussDBMFunction.getRandomFunction();
        int nrArgs = func.getNrArgs();
        if (func.isVariadic()) {
            nrArgs += Randomly.smallNumber();
        }
        GaussDBMExpression[] args = new GaussDBMExpression[nrArgs];
        for (int i = 0; i < args.length; i++) {
            args[i] = generateExpression(depth + 1);
        }
        return new GaussDBMComputableFunction(func, args);
    }

    private enum ConstantType {
        INT, NULL, STRING, DOUBLE;

        public static ConstantType[] valuesPQS() {
            return new ConstantType[]{INT, NULL, STRING};
        }
    }

    @Override
    public GaussDBMExpression generateConstant() {
        ConstantType[] values;
        if (state.usesPQS()) {
            values = ConstantType.valuesPQS();
        } else {
            values = ConstantType.values();
        }
        switch (Randomly.fromOptions(values)) {
            case INT:
                return GaussDBMConstant.createIntConstant((int) state.getRandomly().getInteger());
            case NULL:
                return GaussDBMConstant.createNullConstant();
            case STRING:
                /* Replace characters that still trigger open bugs in GaussDBM */
                String string = state.getRandomly().getString().replace("\\", "").replace("\n", "");
                return GaussDBMConstant.createStringConstant(string);
            case DOUBLE:
                double val = state.getRandomly().getDouble();
                return new GaussDBMDoubleConstant(val);
            default:
                throw new AssertionError();
        }
    }

    @Override
    protected GaussDBMExpression generateColumn() {
        GaussDBMColumn c = Randomly.fromList(columns);
        GaussDBMConstant val;
        if (rowVal == null) {
            val = null;
        } else {
            val = rowVal.getValues().get(c);
        }
        return GaussDBMColumnReference.create(c, val);
    }

    @Override
    public GaussDBMExpression negatePredicate(GaussDBMExpression predicate) {
        return new GaussDBMUnaryPrefixOperation(predicate, GaussDBMUnaryPrefixOperator.NOT);
    }

    @Override
    public GaussDBMExpression isNull(GaussDBMExpression expr) {
        return new GaussDBMUnaryPostfixOperation(expr, GaussDBMUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);
    }

    @Override
    public List<GaussDBMExpression> generateOrderBys() {
        List<GaussDBMExpression> expressions = super.generateOrderBys();
        List<GaussDBMExpression> newOrderBys = new ArrayList<>();
        for (GaussDBMExpression expr : expressions) {
            if (Randomly.getBoolean()) {
                GaussDBMOrderByTerm newExpr = new GaussDBMOrderByTerm(expr, GaussDBMOrder.getRandomOrder());
                newOrderBys.add(newExpr);
            } else {
                newOrderBys.add(expr);
            }
        }
        return newOrderBys;
    }

}
