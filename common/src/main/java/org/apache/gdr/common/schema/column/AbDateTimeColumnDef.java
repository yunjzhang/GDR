package org.apache.gdr.common.schema.column;

import org.apache.gdr.common.conf.Constant;
import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.schema.enums.ColumnProp;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.DataInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public abstract class AbDateTimeColumnDef extends AbColumnDef {
    static final Log LOG = LogFactory.getLog(AbDateTimeColumnDef.class);
    protected SimpleDateFormat sbt;
    protected String datetimeFormat;

    public String getDatetimeFormat() {
        return datetimeFormat;
    }

    public void setDatetimeFormat(String datetimeFormat) {
        this.datetimeFormat = datetimeFormat;
        sbt = new SimpleDateFormat(datetimeFormat);
        sbt.setTimeZone(Constant.DSS_ETL_TIMEZONE);
    }

    @Override
    public String readAsString(DataInputStream dis, boolean isNull) throws IOException, GdrException {
        Object o = read(dis, isNull);
        if (o == null)
            return "";
        else
            return String.valueOf(o);
    }

    @Override
    public String getValueString(Object o) {
        if (o == null)
            return "";
        else
            return String.valueOf(o);
    }

    @Override
    public void setFixValue(String value) {
        try {
            fixValue = sbt.parse(value);
        } catch (ParseException e) {
            throw new GdrRuntimeException(e);
        }
    }

    @Override
    public ObjectNode getGdrSchema() {
        ObjectNode node = super.getGdrSchema();
        node.put(ColumnProp.DATETIME_FORMAT.getName(), this.datetimeFormat);
        return node;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj))
            return Boolean.FALSE;

        AbDateTimeColumnDef c = (AbDateTimeColumnDef) obj;
        if (datetimeFormat == null)
            return datetimeFormat == c.datetimeFormat;
        else
            return datetimeFormat.equals(c.datetimeFormat);
    }

    public String toString() {
        if (datetimeFormat != null) {
            return super.toString() + ",datetimeformat='" + datetimeFormat + "'";
        } else {
            return super.toString();
        }
    }
}
