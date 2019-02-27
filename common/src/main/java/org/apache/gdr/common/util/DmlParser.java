package org.apache.gdr.common.util;

import org.apache.gdr.common.conf.Constant;
import org.apache.gdr.common.datatype.AbDataType;
import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.schema.column.*;
import org.apache.gdr.common.schema.dataset.AbDataDef;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class DmlParser {
    public static final String DmlHead = "RECORD";
    public static final String DmlEnd = "END";
    private static final Log LOG = LogFactory.getLog(DmlParser.class);

    /**
     * @param columnStr
     * @return sample input: datetime(“YYYY-MM-DD HH24:MI:SS”) last_modified;
     */
    public static AbColumnDef parseDatetimeColumn(String columnStr) throws GdrException {
        AbDateTimeColumnDef column = null;
        String subStr = columnStr;

        // get date format
        int p0 = subStr.indexOf("(");
        int p1 = subStr.indexOf(")");

        String typeStr = subStr.substring(0, p0).trim().toUpperCase();
        if (typeStr.contains(AbDataType.DATETIME.getName())) {
            column = new AbTimeStampColumnDef();
            column.setType(AbDataType.DATETIME);
        } else if (typeStr.contains(AbDataType.DATE.getName())) {
            column = new AbDateColumnDef();
            column.setType(AbDataType.DATE);
        }
        //date format
        String dtFormatStr = columnStr.substring(p0 + 1, p1).trim();
        if (dtFormatStr.contains("\'")) {
            column.setDatetimeFormat(
                    AbUtils.tdToJavaDateFormat(StringUtils.substringBetween(dtFormatStr, "\'")));
        } else if (columnStr.trim().contains("\"")) {
            column.setDatetimeFormat(
                    AbUtils.tdToJavaDateFormat(StringUtils.substringBetween(dtFormatStr, "\"")));
        }
        // set string length
        long len = (long) column.getDatetimeFormat().replace("\'", "").length();
        if (column.getDatetimeFormat().contains("a"))
            len = len++;
        if (column.getDatetimeFormat().contains("XX"))
            len = len + 3;
        else if (column.getDatetimeFormat().contains("X"))
            len = len + 2;
        column.setLength(len);

        // get delimiter
        subStr = subStr.substring(p1 + 1).trim();
        if (subStr.startsWith("(")) {
            p0 = subStr.indexOf("(") + 1;
            p1 = subStr.indexOf(")");
            if (p1 > p0) {
                column.setTypeDetail(subStr.substring(p0, p1));
                subStr = subStr.substring(p1 + 1);
            }
        }
        column.setNameAndDefault(subStr);

        return column;
    }

    public static AbDataDef parseDml(DataInputStream in) throws IOException, GdrException {
        return parseDml(AbDataDef.DUMMYNAME, in);
    }

    public static AbDataDef parseDml(String name, DataInputStream in) throws IOException, GdrException {
        StringBuilder sb = new StringBuilder();
        byte[] b = new byte[1024];
        int len = 0;
        while ((len = in.read(b)) > 0) {
            sb.append(new String(b, 0, len));
        }
        return parseDml(name, sb.toString());
    }

    public static AbDataDef parseDml(String name, String dmlStr) throws GdrException {
        AbDataDef dataDef = new AbDataDef(name);
        Charset tableCharset = Constant.AB_DEFAULT_CHARSET;

        if (StringUtils.isNotBlank(dmlStr)) {
            dataDef.setCharset(AbUtils.getCharset(dmlStr));

            String cleanStr = AbUtils.dmlTidy(AbUtils.removeComment(dmlStr));
            String[] columnList = AbUtils.string2Array(cleanStr.trim());
            for (int i = 0; i < columnList.length; i++) {
                AbColumnDef col = null;
                try {
                    col = readAbColumnDef(StringUtils.trim(columnList[i]),
                            dataDef.getCharset() == null ? tableCharset : dataDef.getCharset());
                    dataDef.addColumn(col);
                } catch (GdrException e) {
                    LOG.error("ERROR: bad col[" + i + "]: " + columnList[i]);
                    throw e;
                }
            }
        } else {
            throw new GdrRuntimeException("ERROR: dml string is null");
        }

        dataDef.setDmlStr(dmlStr);

        return dataDef;
    }

    public static AbDataDef parseDml(String dmlStr) throws IOException, GdrException {
        return parseDml(AbDataDef.DUMMYNAME, dmlStr);
    }

    static AbColumnDef parseDecimalColumn(String columnStr) throws GdrException {
        AbColumnDef column = new AbDecimalColumnDef();
        String subStr = columnStr;

        int p0 = subStr.indexOf("(");
        int p1 = subStr.indexOf(")");

        String typeStr = subStr.substring(0, p0).toUpperCase();
        if (typeStr.contains(AbDataType.DECIMAL.getName()))
            column.setType(AbDataType.DECIMAL);

        column.setTypeDetail(subStr.substring(p0 + 1, p1));
        column.setNameAndDefault(subStr.substring(p1 + 1));

        return column;
    }

    static AbColumnDef parseNumericColumn(String columnStr) throws GdrException {
        AbNumberColumnDef column = new AbNumberColumnDef();
        String subStr = columnStr;

        int p0 = subStr.indexOf("(");
        int p1 = subStr.indexOf(")");

        String typeStr = subStr.substring(0, p0).toUpperCase();
        if (typeStr.contains(AbDataType.DECIMAL.getName()))
            column.setType(AbDataType.DECIMAL);
        else if (typeStr.contains(AbDataType.INTEGER.getName()))
            column.setType(AbDataType.INTEGER);
        else if (typeStr.contains(AbDataType.REAL.getName()))
            column.setType(AbDataType.REAL);

        /*
         * handle endian setting as "little endian integer(4) ck_qty = NULL"
         */
        if (typeStr.matches("(?i)(little|big) + endian + (int|real)")
                || typeStr.contains("BIG")) {
            column.setValueEndian(ByteOrder.BIG_ENDIAN);
        }

        /*
         * unsign check as "unsigned integer(8) click_cnt"
         */
        if (typeStr.contains("UNSIGNED")) {
            column.setUnsignValue(Boolean.TRUE);
        }

        if (typeStr.matches("(?i)^ +(little|big) + endian + (int|real)")
                || typeStr.contains("BIG")) {
            column.setValueEndian(ByteOrder.BIG_ENDIAN);
        }

        column.setTypeDetail(subStr.substring(p0 + 1, p1));
        column.setNameAndDefault(subStr.substring(p1 + 1));

        //INTEGER and REAL are binary codec
        if (column.getType().equals(AbDataType.INTEGER) &&
                (column.getLength() == null || column.getLength() < 1))
            column.setLength(4l);
        else if (column.getType().equals(AbDataType.REAL) &&
                (column.getLength() == null || column.getLength() < 1))
            column.setLength(8l);

        return column;
    }

    static AbColumnDef parseStringColumn(String columnStr) throws GdrException {
        return parseStringColumn(columnStr, Constant.AB_DEFAULT_CHARSET);
    }

    static AbColumnDef parseStringColumn(String columnStr, Charset charset) throws GdrException {
        AbStringColumnDef column = new AbStringColumnDef();
        String subStr = columnStr;

        if (AbUtils.getCharset(subStr) != null)
            column.setCharset(AbUtils.getCharset(subStr));
        else if (charset != null)
            column.setCharset(charset);
        else
            column.setCharset(StandardCharsets.ISO_8859_1);

        int p0 = subStr.indexOf("(");
        int p1 = subStr.indexOf(")");
        if (subStr.indexOf("(", p0 + 1) > 0 && subStr.indexOf("(", p0 + 1) < p1) {
            p1 = subStr.indexOf(")", p1 + 1);
        }

        column.setType(AbDataType.STRING);
        column.setTypeDetail(subStr.substring(p0 + 1, p1));
        column.setNameAndDefault(subStr.substring(p1 + 1));

        return column;
    }

    public static AbColumnDef readAbColumnDef(String columnStr, Charset charset) throws GdrException {
        if (columnStr.endsWith(";")) {
            columnStr = columnStr.substring(0, columnStr.length() - 1);
        }

        if (StringUtils.isBlank(columnStr)) {
            return null;
        }

        int typeEndPos = columnStr.indexOf("(");
        String typeStr = typeEndPos > 0 ?
                columnStr.substring(0, typeEndPos).toUpperCase() : columnStr.toUpperCase();

        if (AbUtils.stringContainsAny(typeStr, Constant.SUPPORT_DECIMAL_TYPE)) {
            AbColumnDef column = parseDecimalColumn(columnStr);
            //GDR request decimal including precision and scale
            //and hive support decimal length no more than 38
            if (column.getLength() > 38 || column.getLength() <= 0 || ((AbDecimalColumnDef) column).getScale() == null)
                return parseNumericColumn(columnStr);
            else
                return column;
        } else if (AbUtils.stringContainsAny(typeStr, Constant.SUPPORT_NUMERIC_TYPE)) {
            return parseNumericColumn(columnStr);
        } else if (AbUtils.stringContainsAny(typeStr, Constant.SUPPORT_STRING_TYPE)) {
            return parseStringColumn(columnStr, charset);
        } else if (AbUtils.stringContainsAny(typeStr, Constant.SUPPORT_DATETIME_TYPE)) {
            return parseDatetimeColumn(columnStr);
        } else if (AbUtils.stringContainsAny(typeStr, Constant.SUPPORT_BYTE_TYPE)) {
            return parseByteColumn(columnStr);
        }

        throw new GdrRuntimeException("Unable to parse column: " + columnStr);
    }

    public static AbColumnDef parseByteColumn(String columnStr) throws GdrException {
        AbBytesColumnDef column = new AbBytesColumnDef();
        String subStr = columnStr;

        int p0 = subStr.indexOf("(");
        int p1 = subStr.indexOf(")");
        if (subStr.indexOf("(", p0 + 1) > 0 && subStr.indexOf("(", p0 + 1) < p1) {
            p1 = subStr.indexOf(")", p1 + 1);
        }

        column.setType(AbDataType.VOID);
        column.setTypeDetail(subStr.substring(p0 + 1, p1));
        column.setNameAndDefault(subStr.substring(p1 + 1));

        return column;
    }

    public static AbColumnDef readAbColumnDef(String columnStr) throws IOException, GdrException {
        return readAbColumnDef(columnStr, Constant.AB_DEFAULT_CHARSET);
    }

    public static void main(String[] args) throws IOException, GdrException {
        if (args.length != 3) {
            printUsage(args[0]);
        }

        BufferedReader br = new BufferedReader(new FileReader(args[1]));
        BufferedWriter bw = new BufferedWriter(new FileWriter(args[2]));
        StringBuilder sb = new StringBuilder();

        String sCurrentLine;

        while ((sCurrentLine = br.readLine()) != null) {
            sb.append(sCurrentLine).append("\n");
        }

        String name = args[1].replace(".dml", "");
        String[] strs = name.split("\\.", 3);
        if (strs.length > 1) {
            name = strs[1];
        } else {
            name = strs[0];
        }

        AbDataDef abSchema = DmlParser.parseDml(name, sb.toString());
        LOG.debug(abSchema.getAvroSchema());
        bw.write(abSchema.getAvroSchema().toString());

        br.close();
        bw.close();
    }

    private static void printUsage(String jobName) {
        System.out.println(String.format("Usage: %s <input dml file> <output avro schema>", jobName));
    }
}
