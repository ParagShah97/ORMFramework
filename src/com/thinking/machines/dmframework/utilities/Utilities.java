package com.thinking.machines.dmframework.utilities;

import com.thinking.machines.dmframework.pojo.*;
import java.math.*;
import org.apache.commons.lang3.*;

public class Utilities {
    private Utilities() {
    }

    public static String getJavaClassName(String tableName) {
        return StringUtils.capitalize(getCamelCase(tableName));
    }

    public static String getJavaPropertyName(String fieldName) {
        return getCamelCase(fieldName);
    }

    public static String getCamelCase(String string) {
        string = string.replaceAll("[_]+", " ").trim();
        String[] splits = string.split(" ");
        for (int i = 0; i < splits.length; i++) {
            if (isUpperCase(splits[i]))
                splits[i] = splits[i].toLowerCase();
            if (!isUpperCase(splits[i])) {
                if (i > 0)
                    splits[i] = StringUtils.capitalize(splits[i]);
                else
                    splits[i] = StringUtils.uncapitalize(splits[i]);
            }
        }
        string = StringUtils.join(splits, "");
        return string;
    }

    public static boolean isPrimitive(String dataType) {
        if (dataType.equals("long") || dataType.equals("Long"))
            return true;
        if (dataType.equals("int") || dataType.equals("Integer"))
            return true;
        if (dataType.equals("short") || dataType.equals("Short"))
            return true;
        if (dataType.equals("byte") || dataType.equals("Byte"))
            return true;
        if (dataType.equals("double") || dataType.equals("Double"))
            return true;
        if (dataType.equals("float") || dataType.equals("Float"))
            return true;
        if (dataType.equals("char") || dataType.equals("Character"))
            return true;
        if (dataType.equals("boolean") || dataType.equals("Boolean"))
            return true;
        return false;
    }

    public static boolean isUpperCase(String g) {
        char m;
        for (int i = 0; i < g.length(); i++) {
            m = g.charAt(i);
            if (Character.isLetter(m) && Character.isLowerCase(m))
                return false;
        }
        return true;
    }

    public static int getNumberOfDigits(long n) {
        if (n < 10000L) {
            if (n < 100L) {
                if (n < 10L) {
                    return 1;
                } else {
                    return 2;
                }
            } else {
                if (n < 1000L) {
                    return 3;
                } else {
                    return 4;
                }
            }
        } else {
            if (n < 1000000000000L) {
                if (n < 100000000L) {
                    if (n < 1000000L) {
                        if (n < 100000L) {
                            return 5;
                        } else {
                            return 6;
                        }
                    } else {
                        if (n < 10000000L) {
                            return 7;
                        } else {
                            return 8;
                        }
                    }
                } else {
                    if (n < 10000000000L) {
                        if (n < 1000000000L) {
                            return 9;
                        } else {
                            return 10;
                        }
                    } else {
                        if (n < 100000000000L) {
                            return 11;
                        } else {
                            return 12;
                        }
                    }
                }
            } else {
                if (n < 10000000000000000L) {
                    if (n < 100000000000000L) {
                        if (n < 10000000000000L) {
                            return 13;
                        } else {
                            return 14;
                        }
                    } else {
                        if (n < 1000000000000000L) {
                            return 15;
                        } else {
                            return 16;
                        }
                    }
                } else {
                    if (n < 1000000000000000000L) {
                        if (n < 100000000000000000L) {
                            return 17;
                        } else {
                            return 18;
                        }
                    } else {
                        return 19;
                    }
                }
            }
        }
    }

    public static Pair<Integer, Pair<Integer, Integer>> getBigDecimalLength(BigDecimal b) {
        int totalDigits = b.precision();
        int fractionalDigits = (b.scale() > 0) ? b.scale() : 0;
        int integerDigits = totalDigits - fractionalDigits;
        int totalLength = integerDigits + fractionalDigits;
        if (fractionalDigits > 0)
            totalLength++;
        if (b.signum() == -1)
            totalLength++;
        Pair<Integer, Integer> p1 = new Pair<Integer, Integer>(integerDigits, fractionalDigits);
        Pair<Integer, Pair<Integer, Integer>> p2 = new Pair<Integer, Pair<Integer, Integer>>(totalLength, p1);
        return p2;
    }
}