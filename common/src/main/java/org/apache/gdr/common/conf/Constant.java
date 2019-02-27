package org.apache.gdr.common.conf;

import org.apache.gdr.common.datatype.AbDataType;

import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

public class Constant {
    public static final String DSS_JAVA_TS_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DSS_JAVA_DT_FORMAT = "yyyy-MM-dd";
    public static final String DSS_AB_TS_FORMAT = "YYYY-MM-DD HH24:MI:SS";
    public static final String DSS_AB_DT_FORMAT = "YYYY-MM-DD";
    public static final String DSS_DEFAULT_COL_DELIMITER = "\007";
    public static final String DSS_DEFAULT_LINE_DELIMITER = "\n";
    public static final Set<AbDataType> AB_BINARY_DATA_TYPE = new HashSet<>(Arrays.asList(AbDataType.INTEGER, AbDataType.REAL, AbDataType.VOID));
    public static final ByteOrder DSS_DEFAULT_ENDIAN = ByteOrder.LITTLE_ENDIAN;
    public static final ByteOrder JAVA_DEFAULT_ENDIAN = ByteOrder.BIG_ENDIAN;
    public static final ByteOrder SYSTEM_ENDIAN = ByteOrder.nativeOrder();
    public static final String[] SUPPORT_DECIMAL_TYPE = new String[]{"DECIMAL"};
    public static final String[] SUPPORT_NUMERIC_TYPE = new String[]{"NUMBER", "INTEGER", "REAL"};
    public static final String[] SUPPORT_STRING_TYPE = new String[]{"STRING", "UTF8 STRING"};
    public static final String[] SUPPORT_BYTE_TYPE = new String[]{"VOID"};
    public static final String[] SUPPORT_DATETIME_TYPE = new String[]{"DATE", "DATETIME"};
    public static final String[] IGNORE_COLUMN_NAME = new String[]{"newline"};
    public static final Charset AB_DEFAULT_CHARSET = StandardCharsets.ISO_8859_1;
    public static final TimeZone DSS_ETL_TIMEZONE = TimeZone.getTimeZone("GMT-7:00");
    public static final String DSS_DEFAULT_SCHEMA_NAMESPACE = "com.ebay.dss.adpo.ab";
    public static final Boolean IGNORE_BROKEN_RECORD = Boolean.FALSE;
    public static final Integer DSS_DEFAULT_DECIMAL_PRECISION = 18;
    public static final Integer DSS_DEFAULT_DECIMAL_SCALE = 0;
}
