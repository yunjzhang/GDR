package org.apache.gdr.ext.util;

import org.apache.gdr.common.conf.Constant;
import org.apache.gdr.common.datatype.AbDataType;
import org.apache.gdr.common.datatype.HiveDataType;
import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.schema.column.*;
import org.apache.gdr.common.util.AbUtils;
import org.apache.gdr.hive.util.DmlStyle;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.nio.charset.StandardCharsets;

public class AbColumnDefBuilder {

    /**
     * @param fs
     * @param type
     * @param args array {column delimiter, line delimiter, nvl}
     * @return
     * @throws GdrException
     */
    public static AbColumnDef build(FieldSchema fs, DmlStyle type, String... args) throws GdrException {
        AbColumnDef abC = null;
        String cd = Constant.DSS_DEFAULT_COL_DELIMITER;
        String nvl = "\\N";

        String typeStr = fs.getType();
        String colName = fs.getName();
        String typeProps = null;

        if (typeStr.indexOf("(") > 0) {
            typeStr = typeStr.substring(0, typeStr.indexOf("(")).toUpperCase();
            typeProps = StringUtils.substringBetween(fs.getType(), "(", ")");
        } else {
            typeStr = typeStr.toUpperCase();
        }

        HiveDataType hdt = HiveDataType.valueOf(typeStr);
        AbDataType adt = hdt.asAbType();

        //init column delimiter, line delimiter, nvl
        if (DmlStyle.TEXTDELIMITER.equals(type)) {
            if (args != null && args.length > 0) {
                cd = args[0] == null ? cd : args[0];
                if (args.length > 1) {
                    nvl = args[1] == null ? nvl : args[1];
                }
            }
        }

        if (adt.equals(AbDataType.DECIMAL)) {
            abC = new AbNumberColumnDef();
            abC.setName(colName);
            abC.setType(AbDataType.DECIMAL);
            if (StringUtils.isNotBlank(typeProps)) {
                String[] array = typeProps.trim().split(",");
                abC.setLength(Long.valueOf(array[0].trim()));
                if (array.length > 1)
                    ((AbNumberColumnDef) abC).setPrecision(Integer.valueOf(array[1].trim()));
            }
        } else if (adt.equals(AbDataType.INTEGER)) {
            abC = new AbNumberColumnDef();
            abC.setName(colName);
            if (DmlStyle.TEXTDELIMITER.equals(type)) {
                abC.setType(AbDataType.DECIMAL);
                ((AbNumberColumnDef) abC).setPrecision(0);
            } else {
                abC.setType(AbDataType.INTEGER);
                ((AbNumberColumnDef) abC).setValueEndian(Constant.DSS_DEFAULT_ENDIAN);
                if (hdt.equals(HiveDataType.TINYINT))
                    abC.setLength(1l);
                else if (hdt.equals(HiveDataType.SMALLINT))
                    abC.setLength(2l);
                else if (hdt.equals(HiveDataType.BIGINT))
                    abC.setLength(8l);
                else
                    abC.setLength(4l);
            }
        } else if (adt.equals(AbDataType.REAL)) {
            abC = new AbNumberColumnDef();
            abC.setName(colName);
            if (DmlStyle.TEXTDELIMITER.equals(type)) {
                abC.setType(AbDataType.DECIMAL);
            } else {
                abC.setType(AbDataType.REAL);
                ((AbNumberColumnDef) abC).setValueEndian(Constant.DSS_DEFAULT_ENDIAN);
                if (hdt.equals(HiveDataType.FLOAT))
                    abC.setLength(4l);
                else
                    abC.setLength(8l);
            }
        } else if (adt.equals(AbDataType.STRING)) {
            abC = new AbStringColumnDef();
            abC.setName(colName);
            abC.setType(AbDataType.STRING);
            if (StringUtils.isNotBlank(typeProps))
                abC.setLength(Long.valueOf(typeProps.trim()));
        } else if (adt.equals(AbDataType.DATETIME)) {
            abC = new AbTimeStampColumnDef();
            abC.setName(colName);
            abC.setType(AbDataType.DATETIME);
            ((AbTimeStampColumnDef) abC).setDatetimeFormat(AbUtils.tdToJavaDateFormat(Constant.DSS_AB_TS_FORMAT));
        } else if (adt.equals(AbDataType.DATE)) {
            abC = new AbTimeStampColumnDef();
            abC.setName(colName);
            abC.setType(AbDataType.DATE);
            ((AbTimeStampColumnDef) abC).setDatetimeFormat(AbUtils.tdToJavaDateFormat(Constant.DSS_AB_DT_FORMAT));
        } else if (adt.equals(AbDataType.VOID)) {
            abC = new AbBytesColumnDef();
            abC.setName(colName);
            //abC.setType(AbDataType.VOID);
            abC.setType(AbDataType.STRING);
        } else {
            throw new GdrRuntimeException("unsupporttd Data Type: " + hdt.getName());
        }

        if (DmlStyle.TEXTDELIMITER.equals(type) && !Constant.AB_BINARY_DATA_TYPE.contains(abC.getType()))
            abC.setDelimiter(cd);

        if (abC instanceof AbStringColumnDef)
            ((AbStringColumnDef) abC).setCharset(StandardCharsets.ISO_8859_1);

        abC.setNullable(true);

        abC.setNVL(nvl);

        return abC;
    }
}
