package sqlancer.gaussdbm.ast;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.gaussdbm.GaussDBMSchema.GaussDBMDataType;
import sqlancer.gaussdbm.ast.GaussDBMCastOperation.CastType;

import java.math.BigInteger;

public abstract class GaussDBMConstant implements GaussDBMExpression {

    public boolean isInt() {
        return false;
    }

    public boolean isNull() {
        return false;
    }

    public abstract static class GaussDBMNoPQSConstant extends GaussDBMConstant {

        @Override
        public boolean asBooleanNotNull() {
            throw throwException();
        }

        private RuntimeException throwException() {
            throw new UnsupportedOperationException("not applicable for PQS evaluation!");
        }

        @Override
        public GaussDBMConstant isEquals(GaussDBMConstant rightVal) {
            return null;
        }

        @Override
        public GaussDBMConstant castAs(CastType type) {
            throw throwException();
        }

        @Override
        public String castAsString() {
            throw throwException();

        }

        @Override
        public GaussDBMDataType getType() {
            throw throwException();
        }

        @Override
        protected GaussDBMConstant isLessThan(GaussDBMConstant rightVal) {
            throw throwException();
        }

    }

    public static class GaussDBMDoubleConstant extends GaussDBMNoPQSConstant {

        private final double val;

        public GaussDBMDoubleConstant(double val) {
            this.val = val;
            if (Double.isInfinite(val) || Double.isNaN(val)) {
                // seems to not be supported by GaussDBM
                throw new IgnoreMeException();
            }
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

    }

    public static class GaussDBMTextConstant extends GaussDBMConstant {

        private final String value;
        private final boolean singleQuotes;

        public GaussDBMTextConstant(String value) {
            this.value = value;
            singleQuotes = Randomly.getBoolean();

        }

        private void checkIfSmallFloatingPointText() {
            boolean isSmallFloatingPointText = isString() && asBooleanNotNull()
                    && castAs(CastType.SIGNED).getInt() == 0;
            if (isSmallFloatingPointText) {
                throw new IgnoreMeException();
            }
        }

        @Override
        public boolean asBooleanNotNull() {
            // TODO implement as cast
            for (int i = value.length(); i >= 0; i--) {
                try {
                    String substring = value.substring(0, i);
                    double val = Double.parseDouble(substring);
                    return val != 0 && !Double.isNaN(val);
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
            return false;
            // return castAs(CastType.SIGNED).getInt() != 0;
        }

        @Override
        public String getTextRepresentation() {
            return getString(singleQuotes, value);
        }

        public static String getString(boolean singleQuotes, String value) {
            StringBuilder sb = new StringBuilder();
            String quotes = singleQuotes ? "'" : "\"";
            sb.append(quotes);
            String text = value.replace(quotes, quotes + quotes).replace("\\", "\\\\");
            sb.append(text);
            sb.append(quotes);
            return sb.toString();
        }

        @Override
        public GaussDBMConstant isEquals(GaussDBMConstant rightVal) {
            if (rightVal.isNull()) {
                return GaussDBMConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                checkIfSmallFloatingPointText();
                if (asBooleanNotNull()) {
                    // TODO support SELECT .123 = '.123'; by converting to floating point
                    throw new IgnoreMeException();
                }
                return castAs(CastType.SIGNED).isEquals(rightVal);
            } else if (rightVal.isString()) {
                return GaussDBMConstant.createBoolean(value.equalsIgnoreCase(rightVal.getString()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public String getString() {
            return value;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public GaussDBMConstant castAs(CastType type) {
            if (type == CastType.SIGNED || type == CastType.UNSIGNED) {
                String value = this.value;
                while (value.startsWith(" ") || value.startsWith("\t") || value.startsWith("\n")) {
                    if (value.startsWith("\n")) {
                        /* workaround for https://bugs.GaussDBM.com/bug.php?id=96294 */
                        throw new IgnoreMeException();
                    }
                    value = value.substring(1);
                }
                for (int i = value.length(); i >= 0; i--) {
                    try {
                        String substring = value.substring(0, i);
                        long val = Long.parseLong(substring);
                        return GaussDBMConstant.createIntConstant(val, type == CastType.SIGNED);
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
                return GaussDBMConstant.createIntConstant(0, type == CastType.SIGNED);
            } else {
                throw new AssertionError();
            }
        }

        @Override
        public String castAsString() {
            return value;
        }

        @Override
        public GaussDBMDataType getType() {
            return GaussDBMDataType.VARCHAR;
        }

        @Override
        protected GaussDBMConstant isLessThan(GaussDBMConstant rightVal) {
            if (rightVal.isNull()) {
                return GaussDBMConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                if (asBooleanNotNull()) {
                    // TODO uspport floating point
                    throw new IgnoreMeException();
                }
                checkIfSmallFloatingPointText();
                return castAs(rightVal.isSigned() ? CastType.SIGNED : CastType.UNSIGNED).isLessThan(rightVal);
            } else if (rightVal.isString()) {
                // unexpected result for '-' < "!";
                throw new IgnoreMeException();
            } else {
                throw new AssertionError(rightVal);
            }
        }

    }

    public static class GaussDBMIntConstant extends GaussDBMConstant {

        private final long value;
        private final String stringRepresentation;
        private final boolean isSigned;

        public GaussDBMIntConstant(long value, boolean isSigned) {
            this.value = value;
            this.isSigned = isSigned;
            if (isSigned) {
                stringRepresentation = String.valueOf(value);
            } else {
                stringRepresentation = Long.toUnsignedString(value);
            }
        }

        public GaussDBMIntConstant(long value, String stringRepresentation) {
            this.value = value;
            this.stringRepresentation = stringRepresentation;
            isSigned = true;
        }

        @Override
        public boolean isInt() {
            return true;
        }

        @Override
        public long getInt() {
            return value;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != 0;
        }

        @Override
        public String getTextRepresentation() {
            return stringRepresentation;
        }

        @Override
        public GaussDBMConstant isEquals(GaussDBMConstant rightVal) {
            if (rightVal.isInt()) {
                return GaussDBMConstant.createBoolean(new BigInteger(getStringRepr())
                        .compareTo(new BigInteger(((GaussDBMIntConstant) rightVal).getStringRepr())) == 0);
            } else if (rightVal.isNull()) {
                return GaussDBMConstant.createNullConstant();
            } else if (rightVal.isString()) {
                if (rightVal.asBooleanNotNull()) {
                    // TODO support SELECT .123 = '.123'; by converting to floating point
                    throw new IgnoreMeException();
                }
                return isEquals(rightVal.castAs(CastType.SIGNED));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public GaussDBMConstant castAs(CastType type) {
            if (type == CastType.SIGNED) {
                return new GaussDBMIntConstant(value, true);
            } else if (type == CastType.UNSIGNED) {
                return new GaussDBMIntConstant(value, false);
            } else {
                throw new AssertionError();
            }
        }

        @Override
        public String castAsString() {
            if (isSigned) {
                return String.valueOf(value);
            } else {
                return Long.toUnsignedString(value);
            }
        }

        @Override
        public GaussDBMDataType getType() {
            return GaussDBMDataType.INT;
        }

        @Override
        public boolean isSigned() {
            return isSigned;
        }

        private String getStringRepr() {
            if (isSigned) {
                return String.valueOf(value);
            } else {
                return Long.toUnsignedString(value);
            }
        }

        @Override
        protected GaussDBMConstant isLessThan(GaussDBMConstant rightVal) {
            if (rightVal.isInt()) {
                long intVal = rightVal.getInt();
                if (isSigned && rightVal.isSigned()) {
                    return GaussDBMConstant.createBoolean(value < intVal);
                } else {
                    return GaussDBMConstant.createBoolean(new BigInteger(getStringRepr())
                            .compareTo(new BigInteger(((GaussDBMIntConstant) rightVal).getStringRepr())) < 0);
                    // return GaussDBMConstant.createBoolean(Long.compareUnsigned(value, intVal) < 0);
                }
            } else if (rightVal.isNull()) {
                return GaussDBMConstant.createNullConstant();
            } else if (rightVal.isString()) {
                if (rightVal.asBooleanNotNull()) {
                    // TODO support float
                    throw new IgnoreMeException();
                }
                return isLessThan(rightVal.castAs(isSigned ? CastType.SIGNED : CastType.UNSIGNED));
            } else {
                throw new AssertionError(rightVal);
            }
        }

    }

    public static class GaussDBMNullConstant extends GaussDBMConstant {

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public boolean asBooleanNotNull() {
            throw new UnsupportedOperationException(this.toString());
        }

        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

        @Override
        public GaussDBMConstant isEquals(GaussDBMConstant rightVal) {
            return GaussDBMConstant.createNullConstant();
        }

        @Override
        public GaussDBMConstant castAs(CastType type) {
            return this;
        }

        @Override
        public String castAsString() {
            return "NULL";
        }

        @Override
        public GaussDBMDataType getType() {
            return null;
        }

        @Override
        protected GaussDBMConstant isLessThan(GaussDBMConstant rightVal) {
            return this;
        }

    }

    public long getInt() {
        throw new UnsupportedOperationException();
    }

    public boolean isSigned() {
        return false;
    }

    public String getString() {
        throw new UnsupportedOperationException();
    }

    public boolean isString() {
        return false;
    }

    public static GaussDBMConstant createNullConstant() {
        return new GaussDBMNullConstant();
    }

    public static GaussDBMConstant createIntConstant(long value) {
        return new GaussDBMIntConstant(value, true);
    }

    public static GaussDBMConstant createIntConstant(long value, boolean signed) {
        return new GaussDBMIntConstant(value, signed);
    }

    public static GaussDBMConstant createUnsignedIntConstant(long value) {
        return new GaussDBMIntConstant(value, false);
    }

    public static GaussDBMConstant createIntConstantNotAsBoolean(long value) {
        return new GaussDBMIntConstant(value, String.valueOf(value));
    }

    @Override
    public GaussDBMConstant getExpectedValue() {
        return this;
    }

    public abstract boolean asBooleanNotNull();

    public abstract String getTextRepresentation();

    public static GaussDBMConstant createFalse() {
        return GaussDBMConstant.createIntConstant(0);
    }

    public static GaussDBMConstant createBoolean(boolean isTrue) {
        return GaussDBMConstant.createIntConstant(isTrue ? 1 : 0);
    }

    public static GaussDBMConstant createTrue() {
        return GaussDBMConstant.createIntConstant(1);
    }

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract GaussDBMConstant isEquals(GaussDBMConstant rightVal);

    public abstract GaussDBMConstant castAs(CastType type);

    public abstract String castAsString();

    public static GaussDBMConstant createStringConstant(String string) {
        return new GaussDBMTextConstant(string);
    }

    public abstract GaussDBMDataType getType();

    protected abstract GaussDBMConstant isLessThan(GaussDBMConstant rightVal);

}
