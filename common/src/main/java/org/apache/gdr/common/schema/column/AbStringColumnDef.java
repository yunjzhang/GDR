package org.apache.gdr.common.schema.column;

import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.schema.enums.ColumnProp;
import org.apache.gdr.common.util.AbUtils;
import org.apache.gdr.common.util.StreamSearcher;
import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class AbStringColumnDef extends AbColumnDef {
    static final Log LOG = LogFactory.getLog(AbStringColumnDef.class);

    String outputFormat = null;

    Charset charset = StandardCharsets.UTF_8;
    StreamSearcher streamSearch;

    @Override
    public String convertValue(byte[] input) {
        String s = new String(input, charset);
        return s;
    }

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    @Override
    public void setDelimiter(String delimiter) {
        super.setDelimiter(delimiter);
        streamSearch = new StreamSearcher(delimiter.getBytes());
    }

    @Override
    public String read(InputStream dis, boolean isNull) throws IOException, GdrException {
        if (dis.available() <= 0)
            throw new GdrRuntimeException("unexpected EOF for column: " + this.getName());

        byte[] bytes;

        initBytesRead();

        if (isNull) {
            if (hasBinaryHeader()) {
                bytes = new byte[getLength().intValue()];
                setBytesRead(dis.read(bytes));
            } else if (getDelimiter() != null) {
                bytes = new byte[getDelimiter().length()];
                setBytesRead(getBytesRead() + dis.read(bytes));
            } else if (length > 0) {
                bytes = new byte[length.intValue()];
                setBytesRead(dis.read(bytes));
            }
            return null;
        } else if (hasBinaryHeader()) {
            bytes = new byte[getLength().intValue()];
            setBytesRead(dis.read(bytes));
            if (getLength().intValue() == getBytesRead()) {
                int len = AbUtils.bytesToUnsignInt(bytes, headerEndian);
                //string length must less than 2^31
                if (len > 0) {
                    bytes = new byte[len];
                    int readBytes = dis.read(bytes);
                    if (len != readBytes)
                        throw new GdrRuntimeException("unexpected EOF for column: " + this.getName());
                    else {
                        setBytesRead(getBytesRead() + readBytes);
                        return (String) V2N(convertValue(bytes));
                    }
                } else
                    return "";
            } else
                throw new GdrRuntimeException("unexpected EOF for column: " + this.getName());
        } else if (getDelimiter() == null) {
            bytes = new byte[getLength().intValue()];
            setBytesRead(dis.read(bytes));
            if (getBytesRead() > 0 && !isNull) {
                return (String) V2N(convertValue(bytes));
            }
            return null;
        } else {
            if ((bytes = streamSearch.search(dis)) != null) {
                setBytesRead(bytes.length + delimiter.length());
                if (!isNull)
                    return (String) V2N(convertValue(bytes));
                else
                    return null;
            }

            throw new GdrRuntimeException("unexpected EOF for column: " + this.getName());
        }
    }

    @Override
    public void setFixValue(String value) {
        fixValue = value;
    }

    @Override
    public void write(OutputStream dos, Object value, Boolean defaultToNull) throws IOException {
        byte[] headBytes, dataBytes;
        if (this.fixValue != null)
            dataBytes = fixValue.toString().getBytes();
        else {
            if (outputFormat == null && delimiter == null && !hasBinaryHeader())
                outputFormat = "%" + length + "s";

            String finalValue = (defaultToNull && nvl != null) ?
                    N2V(value).toString() :
                    value == null ?
                            (delimiter != null || hasBinaryHeader()) ? ""
                                    : String.format(outputFormat, "")
                            : value.toString();

            dataBytes = finalValue != null ? finalValue.getBytes(charset) : null;
            if (hasBinaryHeader()) {
                int length = dataBytes == null ? 0 : dataBytes.length;
                headBytes = AbUtils.intToBytes(length, getLength().intValue(), headerEndian);
                dos.write(headBytes);
            } else if (length != null && finalValue != null
                    && length > 0 && finalValue.length() > length) {
                byte[] newValue = new byte[length.intValue()];
                System.arraycopy(dataBytes, 0, dataBytes, 0, length.intValue());
                dataBytes = newValue;
            }
        }

        if (dataBytes != null)
            dos.write(dataBytes);

        if (getDelimiter() != null) {
            dos.write(delimiter.getBytes());
        }
    }

    @Override
    public ObjectNode getGdrSchema() {
        ObjectNode node = super.getGdrSchema();
        if (charset != null)
            node.put(ColumnProp.CHARSET.getName(), charset.toString());
        return node;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj))
            return Boolean.FALSE;

        AbStringColumnDef c = (AbStringColumnDef) obj;
        if (charset == null)
            return c.charset == null;
        else
            return charset.equals(c.charset);
    }

    @Override
    public Schema genColSchema(Boolean logicTypeSupport) {
        colSchema = Schema.create(AbUtils.abTypeToAvroTypeV2(type, 0));
        return colSchema;
    }
}
