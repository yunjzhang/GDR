package org.apache.gdr.common.schema.column;

import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.util.AbUtils;
import org.apache.gdr.common.util.StreamSearcher;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.util.Date;

public class AbTimeStampColumnDef extends AbDateTimeColumnDef {
    static final Log LOG = LogFactory.getLog(AbTimeStampColumnDef.class);

    StreamSearcher streamSearch;

    @Override
    public void setDelimiter(String delimiter) {
        super.setDelimiter(delimiter);
        streamSearch = new StreamSearcher(delimiter.getBytes());
    }

    @Override
    public Long convertValue(byte[] input) throws GdrException {
        if (input == null || input.length < 1)
            return null;
        else if (nvl != null && nvl.toString().equals(new String(input)))
            return null;
        else if (input.length < length)
            throw new GdrRuntimeException("short value (" + new String(input) + ") found for " + name);
        else
            try {
                return sbt.parse(new String(input)).getTime();
            } catch (ParseException e) {
                LOG.error("bad timestamp to be parse:'" + new String(input) + "'.");
                //String s = new String(input);
                throw new GdrRuntimeException("BAD DATETIME", e);
            }
    }

    @Override
    public Long read(InputStream dis, boolean isNull) throws IOException, GdrException {
        if (dis.available() <= 0)
            throw new GdrRuntimeException("unexpected EOF for column: " + this.getName());

        byte[] bytes;
        Long v = null;

        initBytesRead();

        // read value
        if (delimiter == null) {
            bytes = new byte[getLength().intValue()];
            setBytesRead(dis.read(bytes));
            if (getBytesRead() > 0) {
                if (isNull)
                    v = null;
                else
                    v = convertValue(bytes);
            } else
                v = null;
        } else {
            if ((bytes = streamSearch.search(dis)) != null) {
                setBytesRead(bytes.length + delimiter.length());
                if (!isNull)
                    return convertValue(bytes);
                else
                    return null;
            }

            throw new GdrRuntimeException("unexpected EOF for column: " + this.getName());
        }
        return v;
    }

    @Override
    public void write(OutputStream dos, Object value, Boolean defaultToNull) throws IOException {
        if ((value == null && defaultToNull)
                || value != null) {
            dos.write(format(value).getBytes());
        }

        if (getDelimiter() != null) {
            dos.write(delimiter.getBytes());
        }
    }

    @Override
    public String format(Object o) {
        if (o == null && nvl != null)
            return nvl.toString();
        else if (o == null && delimiter == null)
            return String.format("%1$" + length.intValue() + "s", "");
        else if (o instanceof Date)
            return sbt.format((Date) o);
        else if (o instanceof Long)
            return sbt.format(new Date((Long) o));

        return "";
    }

    @Override
    public Schema genColSchema(Boolean logicTypeSupport) {
        if (logicTypeSupport)
            colSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(AbUtils.abTypeToAvroTypeV2(type, getLength().intValue())));
        else
            colSchema = Schema.create(AbUtils.abTypeToAvroTypeV2(type, getLength().intValue()));

        return colSchema;
    }
}
