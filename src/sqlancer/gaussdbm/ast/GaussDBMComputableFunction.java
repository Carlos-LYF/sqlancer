package sqlancer.gaussdbm.ast;

import sqlancer.Randomly;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMDataType;
import sqlancer.gaussdbm.ast.GaussDBMCastOperation.CastType;

import java.util.function.BinaryOperator;
import java.util.stream.Stream;

public class GaussDBMComputableFunction implements GaussDBMExpression {

    private final GaussDBMFunction func;
    private final GaussDBMExpression[] args;

    public GaussDBMComputableFunction(GaussDBMFunction func, GaussDBMExpression... args) {
        this.func = func;
        this.args = args.clone();
    }

    public GaussDBMFunction getFunction() {
        return func;
    }

    public GaussDBMExpression[] getArguments() {
        return args.clone();
    }

    public enum GaussDBMFunction {

        // ABS(1, "ABS") {
        // @Override
        // public GaussDBMConstant apply(GaussDBMConstant[] args, GaussDBMExpression[] origArgs) {
        // if (args[0].isNull()) {
        // return GaussDBMConstant.createNullConstant();
        // }
        // GaussDBMConstant intVal = args[0].castAs(CastType.SIGNED);
        // return GaussDBMConstant.createIntConstant(Math.abs(intVal.getInt()));
        // }
        // },
        /**
         * @see <a href="https://dev.GaussDBM.com/doc/refman/8.0/en/bit-functions.html#function_bit-count">Bit Functions
         * and Operators</a>
         */
//        BIT_COUNT(1, "BIT_COUNT") {
//            @Override
//            public GaussDBMConstant apply(GaussDBMConstant[] evaluatedArgs, GaussDBMExpression... args) {
//                GaussDBMConstant arg = evaluatedArgs[0];
//                if (arg.isNull()) {
//                    return GaussDBMConstant.createNullConstant();
//                } else {
//                    long val = arg.castAs(CastType.SIGNED).getInt();
//                    return GaussDBMConstant.createIntConstant(Long.bitCount(val));
//                }
//            }
//
//        },
        // BENCHMARK(2, "BENCHMARK") {
        //
        // @Override
        // public GaussDBMConstant apply(GaussDBMConstant[] evaluatedArgs, GaussDBMExpression[] args) {
        // if (evaluatedArgs[0].isNull()) {
        // return GaussDBMConstant.createNullConstant();
        // }
        // if (evaluatedArgs[0].castAs(CastType.SIGNED).getInt() < 0) {
        // return GaussDBMConstant.createNullConstant();
        // }
        // if (Math.abs(evaluatedArgs[0].castAs(CastType.SIGNED).getInt()) > 10) {
        // throw new IgnoreMeException();
        // }
        // return GaussDBMConstant.createIntConstant(0);
        // }
        //
        // },
        COALESCE(2, "COALESCE") {
            @Override
            public GaussDBMConstant apply(GaussDBMConstant[] args, GaussDBMExpression... origArgs) {
                GaussDBMConstant result = GaussDBMConstant.createNullConstant();
                for (GaussDBMConstant arg : args) {
                    if (!arg.isNull()) {
                        result = GaussDBMConstant.createStringConstant(arg.castAsString());
                        break;
                    }
                }
                return castToMostGeneralType(result, origArgs);
            }

            @Override
            public boolean isVariadic() {
                return true;
            }

        },
        /**
         * @see <a href="https://dev.GaussDBM.com/doc/refman/8.0/en/control-flow-functions.html#function_if">Flow Control
         * Functions</a>
         */
        IF(3, "IF") {
            @Override
            public GaussDBMConstant apply(GaussDBMConstant[] args, GaussDBMExpression... origArgs) {
                GaussDBMConstant cond = args[0];
                GaussDBMConstant left = args[1];
                GaussDBMConstant right = args[2];
                GaussDBMConstant result;
                if (cond.isNull() || !cond.asBooleanNotNull()) {
                    result = right;
                } else {
                    result = left;
                }
                return castToMostGeneralType(result, origArgs[1], origArgs[2]);

            }

        },
        /**
         * @see <a href="https://dev.GaussDBM.com/doc/refman/8.0/en/control-flow-functions.html#function_ifnull">IFNULL</a>
         */
        IFNULL(2, "IFNULL") {
            @Override
            public GaussDBMConstant apply(GaussDBMConstant[] args, GaussDBMExpression... origArgs) {
                GaussDBMConstant result;
                if (args[0].isNull()) {
                    result = args[1];
                } else {
                    result = args[0];
                }
                return castToMostGeneralType(result, origArgs);
            }

        },
        LEAST(2, "LEAST", true) {
            @Override
            public GaussDBMConstant apply(GaussDBMConstant[] evaluatedArgs, GaussDBMExpression... args) {
                return aggregate(evaluatedArgs, (min, cur) -> cur.isLessThan(min).asBooleanNotNull() ? cur : min);
            }

        },
        GREATEST(2, "GREATEST", true) {
            @Override
            public GaussDBMConstant apply(GaussDBMConstant[] evaluatedArgs, GaussDBMExpression... args) {
                return aggregate(evaluatedArgs, (max, cur) -> cur.isLessThan(max).asBooleanNotNull() ? max : cur);
            }
        };

        private final String functionName;
        final int nrArgs;
        private final boolean variadic;

        private static GaussDBMConstant aggregate(GaussDBMConstant[] evaluatedArgs, BinaryOperator<GaussDBMConstant> op) {
            boolean containsNull = Stream.of(evaluatedArgs).anyMatch(GaussDBMConstant::isNull);
            if (containsNull) {
                return GaussDBMConstant.createNullConstant();
            }
            GaussDBMConstant least = evaluatedArgs[1];
            for (GaussDBMConstant arg : evaluatedArgs) {
                GaussDBMConstant left = castToMostGeneralType(least, evaluatedArgs);
                GaussDBMConstant right = castToMostGeneralType(arg, evaluatedArgs);
                least = op.apply(right, left);
            }
            return castToMostGeneralType(least, evaluatedArgs);
        }

        GaussDBMFunction(int nrArgs, String functionName) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = false;
        }

        GaussDBMFunction(int nrArgs, String functionName, boolean variadic) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = variadic;
        }

        /**
         * Gets the number of arguments if the function is non-variadic. If the function is variadic, the minimum number
         * of arguments is returned.
         *
         * @return the number of arguments
         */
        public int getNrArgs() {
            return nrArgs;
        }

        public abstract GaussDBMConstant apply(GaussDBMConstant[] evaluatedArgs, GaussDBMExpression... args);

        public static GaussDBMFunction getRandomFunction() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String toString() {
            return functionName;
        }

        public boolean isVariadic() {
            return variadic;
        }

        public String getName() {
            return functionName;
        }
    }

    @Override
    public GaussDBMConstant getExpectedValue() {
        GaussDBMConstant[] constants = new GaussDBMConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i].getExpectedValue() == null) {
                return null;
            }
        }
        return func.apply(constants, args);
    }

    public static GaussDBMConstant castToMostGeneralType(GaussDBMConstant cons, GaussDBMExpression... typeExpressions) {
        if (cons.isNull()) {
            return cons;
        }
        GaussDBMDataType type = getMostGeneralType(typeExpressions);
        switch (type) {
            case INT:
                if (cons.isInt()) {
                    return cons;
                } else {
                    return GaussDBMConstant.createIntConstant(cons.castAs(CastType.SIGNED).getInt());
                }
            case VARCHAR:
                return GaussDBMConstant.createStringConstant(cons.castAsString());
            default:
                throw new AssertionError(type);
        }
    }

    public static GaussDBMDataType getMostGeneralType(GaussDBMExpression... expressions) {
        GaussDBMDataType type = null;
        for (GaussDBMExpression expr : expressions) {
            GaussDBMDataType exprType;
            if (expr instanceof GaussDBMColumnReference) {
                exprType = ((GaussDBMColumnReference) expr).getColumn().getType();
            } else {
                exprType = expr.getExpectedValue().getType();
            }
            if (type == null) {
                type = exprType;
            } else if (exprType == GaussDBMDataType.VARCHAR) {
                type = GaussDBMDataType.VARCHAR;
            }

        }
        return type;
    }

}
