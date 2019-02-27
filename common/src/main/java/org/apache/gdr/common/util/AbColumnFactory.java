package org.apache.gdr.common.util;

import org.apache.gdr.common.conf.Constant;
import org.apache.gdr.common.datatype.AbDataType;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.schema.column.*;

import java.util.Arrays;

public class AbColumnFactory {

    public static AbColumnDef build(String type) {
        return build(AbDataType.valueOf(type.toUpperCase()));
    }

    private static AbColumnDef build(AbDataType dataType) {
        AbColumnDef c = null;
        if (Arrays.asList(Constant.SUPPORT_NUMERIC_TYPE).contains(dataType.getName()))
            c = new AbNumberColumnDef();
        else if (Arrays.asList(Constant.SUPPORT_STRING_TYPE).contains(dataType.getName()))
            c = new AbStringColumnDef();
        else if (Arrays.asList(Constant.SUPPORT_DATETIME_TYPE).contains(dataType.getName())) {
            if (dataType.equals(AbDataType.DATE))
                c = new AbDateColumnDef();
            else if (dataType.equals(AbDataType.DATETIME))
                c = new AbTimeStampColumnDef();
        } else if (Arrays.asList(Constant.SUPPORT_DECIMAL_TYPE).contains(dataType.getName()))
            c = new AbDecimalColumnDef();
        else if (Arrays.asList(Constant.SUPPORT_BYTE_TYPE).contains(dataType.getName()))
            c = new AbBytesColumnDef();
        else
            throw new GdrRuntimeException("Unsupported GDR data type: " + dataType);

        if (c != null)
            c.setType(dataType);

        return c;
    }
}
