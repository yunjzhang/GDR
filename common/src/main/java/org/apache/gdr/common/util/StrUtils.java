package org.apache.gdr.common.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class StrUtils {
    /**
     * @param value
     * @return string without leading/tailing space
     */
    public static String trim(String value) {
        return trim(value, ' ');
    }

    /**
     * @param value
     * @return string without leading/tailing specified chars
     */
    public static String trim(String value, String dels) {
        int len = value.length();
        int st = 0;
        char[] delArray = new char[dels.length()];
        dels.getChars(0, dels.length(), delArray, 0);
        Set set = new HashSet(Arrays.asList(delArray));
        char[] val = value.toCharArray();    /* avoid getfield opcode */

        while (st < len && set.contains(val[st])) {
            st++;
        }
        while (st < len && set.contains(val[st])) {
            len--;
        }
        return ((st > 0) || (len < value.length())) ? value.substring(st, len) : value;
    }

    /**
     * @param value
     * @return string without leading/tailing specified char
     */
    public static String trim(String value, char del) {
        int len = value.length();
        int st = 0;
        char[] val = value.toCharArray();    /* avoid getfield opcode */

        while ((st < len) && (val[st] == del)) {
            st++;
        }
        while ((st < len) && (val[len - 1] == del)) {
            len--;
        }
        return ((st > 0) || (len < value.length())) ? value.substring(st, len) : value;
    }
}
