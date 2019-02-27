package org.apache.gdr.ext.util;

import org.apache.gdr.common.datatype.HiveDataType;
import org.apache.gdr.common.datatype.TdDataType;
import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.List;

public class TdDdlBuilder {
    static final int STRINGLEN = 100;
    static final int BYTELEN = 32;
    static Log LOG = LogFactory.getLog(TdDdlBuilder.class);

    public static String build(HiveTableDefinition ctd) throws GdrException {

        StringBuilder sb = new StringBuilder();
        //table name
        sb.append("CREATE MULTISET TABLE working.").append(ctd.getTableName()).append("\n");

        //table columns
        sb.append("(\n");

        List<FieldSchema> columnList = ctd.getCols();

        String colName = null, firstColName = null;
        String typeStr = null, typePropStr = null;
        String colComm = null;
        TdDataType tdt = null;

        int i = 0;

        for (FieldSchema fs : columnList) {
            i++;

            typeStr = fs.getType();
            colName = fs.getName();
            colComm = fs.getComment();

            sb.append("  ").append(colName).append(" ");

            if (i == 1)
                firstColName = fs.getName();

            if (typeStr.indexOf("(") > 0) {
                typePropStr = typeStr.substring(typeStr.indexOf("("));
                typeStr = typeStr.substring(0, typeStr.indexOf("(")).toUpperCase();
            } else {
                typePropStr = null;
                typeStr = typeStr.toUpperCase();
            }

            tdt = HiveDataType.valueOf(typeStr).asTdType();

            sb.append(tdt.getName());

            if (tdt.equals(TdDataType.DECIMAL)) {
                if (typePropStr == null)
                    throw new GdrRuntimeException("Error: missing scale or precision for DECIMAL field:" + fs.getName());
                sb.append(typePropStr);
            } else if (tdt.equals(TdDataType.BYTE)
                    || tdt.equals(TdDataType.VARBYTE)) {
                if (typePropStr == null) {
                    sb.append("(" + BYTELEN + ")");
                    LOG.warn("Field " + colName + " is defined as " + typeStr + ", assigning default length " + BYTELEN);
                    //throw new GdrRuntimeException("Error: missing length for string field:" + fs.getName());
                } else {
                    sb.append(typePropStr);
                }
            } else if (tdt.equals(TdDataType.CHAR)
                    || tdt.equals(TdDataType.VARCHAR)) {
                if (typePropStr == null) {
                    sb.append("(" + STRINGLEN + ")");
                    LOG.warn("Field " + colName + " is defined as " + typeStr + ", assigning default length " + STRINGLEN);
                    //throw new GdrRuntimeException("Error: missing length for string field:" + fs.getName());
                } else {
                    sb.append(typePropStr);
                }
            } else if (tdt.equals(TdDataType.INTEGER)
                    || tdt.equals(TdDataType.SMALLINT)
                    || tdt.equals(TdDataType.BYTEINT)
                    || tdt.equals(TdDataType.BIGINT)) {
                //void
            } else if (tdt.equals(TdDataType.FLOAT)
                    || tdt.equals(TdDataType.DOUBLE)
                    || tdt.equals(TdDataType.REAL)) {
                //void
            } else if (tdt.equals(TdDataType.DATE)) {
                sb.append(" FORMAT 'YYYY-MM-DD'");
            } else if (tdt.equals(TdDataType.TIME)) {
                sb.append("(0) FORMAT 'HH:MI:SS'");
            } else if (tdt.equals(TdDataType.TIMESTAMP)) {
                sb.append("(0) FORMAT 'YYYY-MM-DDbHH:MI:SS'");
            } else {
                throw new GdrRuntimeException("unsupporttd Data Type: " + typeStr);
            }

            if (colComm != null)
                sb.append(colComm);
            if (i < columnList.size())
                sb.append(",\n");
            else
                sb.append("\n");
        }

        sb.append(")\n");

        //pi columns
        sb.append("PRIMARY INDEX TPI").append(ctd.getTableName())
                .append("(").append(firstColName).append(");\n");
        LOG.warn("Set first field " + colName + " as PI for table" + ctd.getTableName());

        return sb.toString();
    }
}
