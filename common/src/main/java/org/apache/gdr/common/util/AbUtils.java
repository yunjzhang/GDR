package org.apache.gdr.common.util;

import org.apache.gdr.common.conf.Constant;
import org.apache.gdr.common.datatype.AbDataType;
import org.apache.gdr.common.exception.AbTypeException;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.avro.Schema.Type;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AbUtils {
    public static String[] string2Array(String dmlString) {
        if (!dmlString.contains("\n"))
            return dmlString.trim().split(";");
        else
            return dmlString.trim().split("\n");
    }

    /**
     * user google java formatter temporary, may replace with a dedicated dml
     * formatter later
     *
     * @throws FormatterException
     */
    public static String dmlTidy(String dmlStr) {
        String subStr = dmlTidy2(dmlStr).trim();

        if (subStr == null)
            return subStr;
        else {
            int returnCnt = AbUtils.countString(dmlStr, "\n");
            int scCnt = AbUtils.countString(dmlStr, ";");
            if (returnCnt > scCnt)
                return subStr.replace("\n", "").replace("\r", "");
            else
                return subStr;
        }
    }

    public static String dmlTidy2(String dmlStr) {
        if (StringUtils.isBlank(dmlStr)) {
            return null;
        }
        int tailPos = -1;
        int headPos = -1;
        int prePos = -1;
        int postPos = -1;
        boolean findKey = Boolean.FALSE;

        while (!findKey) {
            // index key word 'record'
            headPos = StringUtils.indexOfIgnoreCase(dmlStr, DmlParser.DmlHead, headPos + 1);
            if (headPos < 0)
                throw new GdrRuntimeException("dml head is not found, pls check.");
            // check whole word 'record'
            prePos = headPos == 0 ? headPos : headPos - 1;
            postPos = headPos + DmlParser.DmlHead.length() == dmlStr.length() ? headPos + DmlParser.DmlHead.length()
                    : headPos + DmlParser.DmlHead.length() + 1;
            if (dmlStr.substring(prePos, postPos).trim().toUpperCase().equals(DmlParser.DmlHead))
                findKey = Boolean.TRUE;
            else
                findKey = Boolean.FALSE;
        }

        findKey = Boolean.FALSE;
        while (!findKey) {
            tailPos = StringUtils.lastIndexOfIgnoreCase(dmlStr, DmlParser.DmlEnd);
            if (tailPos < 0)
                throw new GdrRuntimeException("dml tail is not found, pls check.");
            // check whole word 'record'
            prePos = tailPos == 0 ? tailPos : tailPos - 1;
            postPos = tailPos + DmlParser.DmlEnd.length() == dmlStr.length() ? tailPos + DmlParser.DmlEnd.length()
                    : tailPos + DmlParser.DmlEnd.length() + 1;
            if (dmlStr.substring(prePos, postPos).trim().replace(";", "").toUpperCase().equals(DmlParser.DmlEnd))
                findKey = Boolean.TRUE;
            else
                dmlStr = dmlStr.substring(0, tailPos);
        }

        String subStr = StringUtils.substring(dmlStr, headPos + DmlParser.DmlHead.length() + 1, tailPos);
        // TODO format before return

        return subStr;
    }

    public static String removeCommentWithReg(String str) {
        if (StringUtils.isBlank(str)) {
            return null;
        }
        return str.replaceAll("(?:/\\*(?:[^*]|(?:\\*+[^*/]))*\\*+/)|(?://.*)", "");
    }

    public static Type abTypeToAvroTypeV2(AbDataType type, Integer... len) {
        switch (type) {
            case STRING:
                return Type.STRING;
            case INTEGER:
                switch (len[0]) {
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                        return Type.INT;
                    case 5:
                    case 6:
                    case 7:
                    case 8:
                        return Type.LONG;
                    default:
                        return Type.INT;
                }
            case REAL:
                switch (len[0]) {
                    case 4:
                        return Type.FLOAT;
                    case 8:
                        return Type.DOUBLE;
                    default:
                        return Type.FLOAT;
                }
            case DECIMAL:
                return Type.BYTES; //logic type DECIMAL
            case VOID:
                return Type.BYTES;
            case DATE:
                return Type.INT;
            case DATETIME:
                return Type.LONG;
            case RECORD:
                return Type.RECORD;
            case UNION:
                return Type.UNION;
            case VECTOR:
                return Type.ARRAY;
            default:
                return null;
        }
    }

    public static Type abTypeToAvroTypeV1(AbDataType type, Integer... len) {
        switch (type) {
            case STRING:
                return Type.STRING;
            case INTEGER:
                switch (len[0]) {
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                        return Type.INT;
                    case 5:
                    case 6:
                    case 7:
                    case 8:
                        return Type.LONG;
                    default:
                        return Type.INT;
                }
            case REAL:
                switch (len[0]) {
                    case 4:
                        return Type.FLOAT;
                    case 8:
                        return Type.DOUBLE;
                    default:
                        return Type.FLOAT;
                }
            case DECIMAL:
                return (len.length == 2 && len[1] != null && len[1] == 0) ? Type.LONG : Type.DOUBLE;
            case VOID:
                return Type.BYTES;
            case DATE:
                return Type.INT;
            case DATETIME:
                return Type.LONG;
            case RECORD:
                return Type.RECORD;
            case UNION:
                return Type.UNION;
            case VECTOR:
                return Type.ARRAY;
            default:
                return null;
        }
    }

    @SuppressWarnings("rawtypes")
    public static Class abTypeToJavaType(AbDataType type, Integer len, Boolean unsign) throws AbTypeException {
        switch (type) {
            case STRING:
                return String.class;
            case INTEGER:
                switch (len) {
                    case 1:
                        if (!unsign)
                            return Byte.class;
                        else
                            return Short.class;
                    case 2:
                        if (!unsign)
                            return Short.class;
                        else
                            return Integer.class;
                    case 3:
                        return Integer.class;
                    case 4:
                        if (!unsign)
                            return Integer.class;
                        else
                            return Long.class;
                    case 5:
                    case 6:
                    case 7:
                    case 8:
                        return Long.class;
                    default:
                        return Integer.class;
                }
            case REAL:
                switch (len) {
                    case 4:
                        return Float.class;
                    case 8:
                        return Double.class;
                    default:
                        return Float.class;
                }
            case DECIMAL:
                return Double.class;
            case VOID:
                return byte[].class;
            case DATE:
            case DATETIME:
                return Long.class;
            case RECORD:
            case UNION:
            case VECTOR:
            default:
                throw new AbTypeException("ERROR: unsupport ab type in Java:" + type.getName());
        }
    }

    public static String tdToJavaDateFormat(String tdFormat) {
        if (StringUtils.isBlank(tdFormat))
            throw new GdrRuntimeException("blank string to convert.");
        String outFormat = tdFormat.toUpperCase();

        if (StringUtils.containsIgnoreCase(outFormat, "T"))
            outFormat = outFormat.replaceAll("(?i)T", "\'T\'");
        if (StringUtils.containsIgnoreCase(outFormat, "V"))
            outFormat = outFormat.replaceAll("(?i)V", "\'V\'");
        if (StringUtils.containsIgnoreCase(outFormat, "AM"))
            outFormat = outFormat.replaceAll("(?i)AM", "\'a\'");
        if (StringUtils.containsIgnoreCase(outFormat, "+zo:ne"))
            outFormat = outFormat.replaceAll("(?i)\\+zo:ne", "XXX");

        outFormat = outFormat.replace("Y", "y").replace("DD", "dd")
                .replace("SS", "ss").replace("MI", "mm")
                .replace("N", "S");

        if (StringUtils.containsIgnoreCase(outFormat, "HH24"))
            outFormat = outFormat.replace("HH24", "HH");
        else if (StringUtils.containsIgnoreCase(outFormat, "HH"))
            outFormat = outFormat.replace("HH", "hh");

        return outFormat;
    }

    public static byte[] reverseByteArray(byte[] bytes) {
        for (int i = 0; i < bytes.length / 2; i++) {
            byte temp = bytes[i];
            bytes[i] = bytes[bytes.length - i - 1];
            bytes[bytes.length - i - 1] = temp;
        }
        return bytes;
    }

    public static char convertUnicodeChar(String s) {
        int ret = 0;
        if (s.charAt(0) == 'u') {
            for (int i = 1; i < s.length() && i < 5; i++) {
                int curr = Integer.parseInt("" + s.charAt(i), 16);
                ret = (ret << 4) | curr;
            }
        }
        return (char) ret;
    }

    public static String bytesToHex(byte[] in) {
        final StringBuilder builder = new StringBuilder();
        for (byte b : in) {
            builder.append(String.format("%02x", b));
        }
        return builder.toString();
    }

    public static String bytesToOct(byte[] in) {
        final StringBuilder builder = new StringBuilder();
        for (byte b : in) {
            builder.append(String.format("\\%03o", b));
        }
        return builder.toString();
    }

    public static String bytesToUnicode(byte[] in) {
        final StringBuilder builder = new StringBuilder();
        for (byte b : in) {
            builder.append(String.format("\\u%04x", b));
        }
        return builder.toString();
    }

    public static byte[] intToBytes(Integer i) {
        return intToBytes(i, Constant.DSS_DEFAULT_ENDIAN);
    }

    @Deprecated
    public static byte[] intToBytes(Integer i, Integer len) {
        return intToBytes(i, len, Constant.DSS_DEFAULT_ENDIAN);
    }

    public static byte[] intToBytes(Integer i, ByteOrder endian) {
        ByteBuffer bb = ByteBuffer.allocate(4).order(endian);
        bb.putInt(i);
        return bb.array();
    }

    public static byte[] intToBytes(Integer i, Integer len, ByteOrder endian) {
        byte[] bytes = intToBytes(i, endian);
        byte[] obs = null;
        if (len < 4 && len > 0) {
            obs = new byte[len];
            if (ByteOrder.BIG_ENDIAN.equals(endian))
                System.arraycopy(bytes, bytes.length - obs.length, obs, 0, obs.length);
            else
                System.arraycopy(bytes, 0, obs, 0, obs.length);
        } else {
            obs = bytes;
        }
        return obs;
    }

    public static byte[] longToBytes(Long l) {
        return longToBytes(l, Constant.DSS_DEFAULT_ENDIAN);
    }

    public static byte[] longToBytes(Long l, Integer len) {
        return longToBytes(l, len, Constant.DSS_DEFAULT_ENDIAN);
    }

    public static byte[] longToBytes(Long l, ByteOrder endian) {
        ByteBuffer bb = ByteBuffer.allocate(8).order(endian);
        bb.putLong(l);
        return bb.array();
    }

    public static byte[] longToBytes(Long i, Integer len, ByteOrder endian) {
        byte[] bytes = longToBytes(i, endian);
        byte[] obs = null;
        if (len < 8 && len > 0) {
            obs = new byte[len];
            if (ByteOrder.BIG_ENDIAN.equals(endian))
                System.arraycopy(bytes, bytes.length - obs.length, obs, 0, obs.length);
            else
                System.arraycopy(bytes, 0, obs, 0, obs.length);
        } else {
            obs = bytes;
        }
        return obs;
    }

    public static Integer bytesToUnsignInt(byte[] bs) {
        return bytesToUnsignInt(bs, Constant.DSS_DEFAULT_ENDIAN);
    }

    public static Integer bytesToUnsignInt(byte[] bs, ByteOrder endian) {
        byte[] bytes = null;

        if (ByteOrder.LITTLE_ENDIAN.equals(endian))
            bytes = reverseByteArray(bs);
        else
            bytes = bs;

        Long value = 0l;
        for (int i = 0; i < bytes.length; i++) {
            value = (value << 8) + (bytes[i] & 0xff);
        }
        return value.intValue();
    }

    public static Long bytesToLong(byte[] bs, ByteOrder endian) {
        byte[] bytes = null;

        if (ByteOrder.LITTLE_ENDIAN.equals(endian))
            bytes = reverseByteArray(bs);
        else
            bytes = bs;

        Long value = 0l;
        for (int i = 0; i < bytes.length; i++) {
            value = (value << 8) + (bytes[i] & 0xff);
        }
        return value.longValue();
    }

    public static boolean compareByteArray(byte[] b1, byte[] b2) {
        if (b1 == null && b2 == null)
            return Boolean.TRUE;
        if (b1 == null || b2 == null)
            return Boolean.FALSE;

        if (b1.length != b2.length)
            return Boolean.FALSE;

        for (int i = 0; i < b1.length; i++)
            if (b1[i] != b2[i])
                return Boolean.FALSE;

        return Boolean.TRUE;
    }

    public static String bytes2BitString(byte[] bytes) {
        String out = "";
        for (byte b : bytes) {
            out = out.concat(byte2BitString(b)).concat(" ");
        }
        return out;
    }

    public static String byte2BitString(byte b) {
        return Integer.toBinaryString(b & 255 | 256).substring(1);
    }

    public static int bitAt(final byte[] isNullMask, final int pos) {
        return (isNullMask[pos / 8] & (1 << pos % 8)) > 0 ? 1 : 0;
    }

    public static void setBit(byte[] isNullMask, final int pos, boolean isNull) {
        if (isNull)
            isNullMask[pos / 8] = (byte) (isNullMask[pos / 8] | (1 << pos % 8));
        else
            isNullMask[pos / 8] = (byte) (isNullMask[pos / 8] & ~(1 << pos % 8));
    }

    public static String decodeString(String inStr) {
        String tmp = inStr.replace("\\n", "\n").replace("\\r", "\r")
                .replace("\\a", "\007").replace("\\b", "\010")
                .replace("\\t", "\011").replace("\\v", "\013")
                .replace("\\f", "\014");
        StringBuffer out = new StringBuffer();
        // unicode replacement
        Matcher m = Pattern.compile("\\\\(u\\d{1,4})").matcher(tmp);
        while (m.find()) {
            m.appendReplacement(out, String.valueOf(AbUtils.convertUnicodeChar(m.group(1))));
        }
        m.appendTail(out);
        // oct replacement
        tmp = out.toString();
        out = new StringBuffer();
        m = Pattern.compile("\\\\(\\d{1,3})").matcher(tmp);
        while (m.find()) {
            m.appendReplacement(out, String.valueOf((char) Integer.parseInt(m.group(1), 8)));
        }
        m.appendTail(out);
        // hex replacement
        tmp = out.toString();
        out = new StringBuffer();
        m = Pattern.compile("\\\\(x[A-Fa-f0-9]{1,2})").matcher(tmp);
        while (m.find()) {
            m.appendReplacement(out, String.valueOf((char) Integer.parseInt(m.group(1).substring(1), 16)));
        }
        m.appendTail(out);
        return out.toString();
    }

    public static String encodeString(String inStr) {
        StringBuffer out = new StringBuffer();
        String s = inStr.replace("\n", "\\n");
        for (int i = 0; i < s.length(); i++) {
            String c = s.substring(i, i + 1);
            if (StringUtils.isAsciiPrintable(c))
                out.append(c);
            else
                out.append(bytesToUnicode(c.getBytes()));
        }
        return out.toString();
    }

    public static boolean stringContainsAny(String inputStr, String[] items) {
        for (int i = 0; i < items.length; i++) {
            if (inputStr.contains(items[i])) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    public static String getQuoteString(String str) {
        if (StringUtils.isBlank(str))
            return null;

        // check "
        if (str.startsWith("\"") && str.endsWith("\""))
            return StringUtils.substringBetween(str, "\"");
        else if (str.startsWith("\'") && str.endsWith("\'"))
            return StringUtils.substringBetween(str, "\'");
        else
            return str;
    }

    public static int countString(String str, String findStr) {
        return countString(str, findStr, Boolean.FALSE);
    }

    public static int countString(String str, String findStr, Boolean overlap) {
        int lastIndex = 0;
        int count = 0;
        int len = overlap ? 0 : findStr.length();

        while (lastIndex != -1) {

            lastIndex = str.indexOf(findStr, lastIndex + len);

            if (lastIndex != -1) {
                count++;
                lastIndex += findStr.length();
            }
        }
        return count;
    }

    public static String removeIllegalAvroChar(String name) {
        if (name == null)
            return null;
        else
            return name.replace("-", "_");
    }

    @SuppressWarnings("rawtypes")
    public static String getJarVersion(Class clazz) {
        return clazz.getPackage().getSpecificationVersion();
    }

    public static String removeComment(String code) {
        return removeComment(code, CommentType.JAVA);
    }

    public static String removeComment(String code, CommentType type) {
        final int outsideComment = 0;
        final int insideLineComment = 1;
        final int insideblockComment = 2;
        final int insideblockComment_noNewLineYet = 3;
        final int insideQuotation = 4;

        final String blockCommentFirstChar = type.getBlockCommentFirstChar();
        final String blockCommentSecondChar = type.getBlockCommentSecondChar();
        final String blockCommentThirdChar = type.getBlockCommentThirdChar();
        final String blockCommentForthChar = type.getBlockCommentForthChar();
        final String lineCommentFirstChar = type.getLineCommentFirstChar();
        final String lineCommentSecondChar = type.getLineCommentSecondChar();
        final String lineDelimiter = type.getLineDelimiter();
        final Set<String> quotationCharList = type.getQuotationCharList();

        int currentState = outsideComment;
        String currentQuotationMark = "";
        StringBuilder endResult = new StringBuilder();
        Scanner s = new Scanner(code);
        s.useDelimiter("");
        while (s.hasNext()) {
            String c = s.next();
            switch (currentState) {
                case outsideComment:
                    if (quotationCharList.contains(c)) {
                        currentState = insideQuotation;
                        currentQuotationMark = c;
                        endResult.append(c);
                        break;
                    } else if (!blockCommentFirstChar.equals(c) && !lineCommentFirstChar.equals(c)) {
                        endResult.append(c);
                        break;
                    } else {
                        while ((blockCommentFirstChar.equals(c) || lineCommentFirstChar.equals(c))
                                && s.hasNext()) {
                            String c2 = s.next();
                            if (blockCommentFirstChar.equals(c) && blockCommentSecondChar.equals(c2)) {
                                currentState = insideblockComment_noNewLineYet;
                                break;
                            } else if (lineCommentFirstChar.equals(c) && lineCommentSecondChar.equals(c2)) {
                                currentState = insideLineComment;
                                break;
                            } else {
                                endResult.append(c);
                                c = c2;
                            }
                        }

                        if (!blockCommentFirstChar.equals(c) && !lineCommentFirstChar.equals(c))
                            endResult.append(c);

                        break;
                    }
                case insideQuotation:
                    if (currentQuotationMark.equals(c)) {
                        currentState = outsideComment;
                        currentQuotationMark = "";
                    }
                    endResult.append(c);
                    break;
                case insideLineComment:
                    if (c.equals(lineDelimiter)) {
                        currentState = outsideComment;
                        endResult.append(lineDelimiter);
                    }
                    break;
                case insideblockComment_noNewLineYet:
                    if (c.equals(lineDelimiter)) {
                        //endResult.append(lineDelimiter);
                        currentState = insideblockComment;
                    }
                case insideblockComment:
                    while (blockCommentThirdChar.equals(c) && s.hasNext()) {
                        String c2 = s.next();
                        if (blockCommentForthChar.equals(c2)) {
                            currentState = outsideComment;
                            break;
                        } else
                            c = c2;
                    }
            }
        }
        s.close();
        return endResult.toString();
    }

    public static Charset getCharset(String subStr) {
        String str = removeComment(subStr).trim();
        if (StringUtils.isBlank(str)) {
            return null;
        } else if (str.startsWith("utf8")) {
            return StandardCharsets.UTF_8;
        } else if (str.startsWith("ascii")) {
            return StandardCharsets.ISO_8859_1;
        } else if (str.startsWith("unicode")) {
            return StandardCharsets.UTF_16;
        } else if (str.startsWith("big endian unicode")) {
            return StandardCharsets.UTF_16BE;
        } else if (str.startsWith("little endian unicode")) {
            return StandardCharsets.UTF_16LE;
        } else {
            return null;
        }
    }

    public static byte[] doubleToBytes(Double value) {
        return doubleToBytes(value, Constant.DSS_DEFAULT_ENDIAN);
    }

    public static byte[] doubleToBytes(Double value, int len) {
        return doubleToBytes(value, len, Constant.DSS_DEFAULT_ENDIAN);
    }

    public static byte[] doubleToBytes(Double value, ByteOrder endian) {
        ByteBuffer bb = ByteBuffer.allocate(8).order(endian);
        bb.putDouble(value);
        return bb.array();
    }

    public static byte[] doubleToBytes(Double value, int len, ByteOrder endian) {
        byte[] bytes = doubleToBytes(value, endian);
        byte[] obs = null;
        if (len < 8 && len > 0) {
            obs = new byte[len];
            if (ByteOrder.BIG_ENDIAN.equals(endian))
                System.arraycopy(bytes, bytes.length - obs.length, obs, 0, obs.length);
            else
                System.arraycopy(bytes, 0, obs, 0, obs.length);
        } else {
            obs = bytes;
        }
        return obs;
    }
}
