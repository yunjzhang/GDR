package org.apache.gdr.common.schema.column;

import org.apache.gdr.common.conf.Constant;
import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.util.AbUtils;
import org.apache.gdr.common.util.StreamSearcher;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class AbBytesColumnDef extends AbColumnDef {
    static final Log LOG = LogFactory.getLog(AbBytesColumnDef.class);
    static final Charset charset = Constant.AB_DEFAULT_CHARSET;

    StreamSearcher streamSearch;

    @Override
    public byte[] convertValue(byte[] input) {
        return AbUtils.compareByteArray(input, (byte[]) nvl) ? null : input;
    }

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        if (!charset.equals(Constant.AB_DEFAULT_CHARSET))
            throw new GdrRuntimeException("bytes column cannot support any charset.");
    }

    @Override
    public void setDelimiter(String delimiter) {
        super.setDelimiter(delimiter);
        streamSearch = new StreamSearcher(delimiter.getBytes());
    }

    @Override
    public ByteBuffer read(InputStream dis, boolean isNull) throws IOException, GdrException {
        if (dis.available() <= 0)
            throw new GdrRuntimeException("unexpected EOF for column: " + this.getName());

        byte[] bytes;

        initBytesRead();

        if (isNull) {
            if (hasBinaryHeader()) {
                bytes = new byte[getLength().intValue()];
                setBytesRead(dis.read(bytes));
            }

            if (getDelimiter() != null) {
                bytes = new byte[getDelimiter().length()];
                setBytesRead(getBytesRead() + dis.read(bytes));
            }
            return null;
        } else if (hasBinaryHeader()) {
            bytes = new byte[getLength().intValue()];
            setBytesRead(dis.read(bytes));
            if (getBytesRead() > 0) {
                int len = AbUtils.bytesToUnsignInt(bytes, headerEndian);
                if (len > 0) {
                    bytes = new byte[len];
                    len = dis.read(bytes);
                    return byte2ByteBuffer(bytes);
                } else
                    return null;
            }
            return null;
        } else if (getDelimiter() == null) {
            bytes = new byte[getLength().intValue()];
            setBytesRead(dis.read(bytes));
            if (getBytesRead() == getLength().intValue()) {
                return byte2ByteBuffer(convertValue(bytes));
            } else
                throw new EOFException("Unexpected EOF when reading " + getName());
            //return null;
        } else {
            if ((bytes = streamSearch.search(dis)) != null) {
                setBytesRead(bytes.length + delimiter.length());
                return byte2ByteBuffer(convertValue(bytes));
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
        byte[] bb = value == null ? null : ((ByteBuffer) value).array();
        if (this.fixValue != null)
            dataBytes = (byte[]) fixValue;
        else {
            dataBytes = (byte[]) ((defaultToNull && nvl != null) ? N2V(bb) : bb);

            if (hasBinaryHeader()) {
                int length = dataBytes == null ? 0 : dataBytes.length;
                headBytes = AbUtils.intToBytes(length, getLength().intValue(), headerEndian);
                dos.write(headBytes);
            } else if (length > 0 && dataBytes != null) {
                byte[] newValue = new byte[length.intValue()];
                if (dataBytes.length >= length) {
                    System.arraycopy(dataBytes, 0, dataBytes, 0, length.intValue());
                } else {
                    System.arraycopy(dataBytes, 0, newValue, 0, dataBytes.length);
                }
                dataBytes = newValue;
            }
        }

        if (dataBytes != null)
            dos.write(dataBytes);

        if (getDelimiter() != null) {
            dos.write(delimiter.getBytes());
        }
    }

    protected ByteBuffer byte2ByteBuffer(byte[] input) {
        if (input == null)
            return null;
        return ByteBuffer.wrap(input);

    }

    @Override
    public String format(Object o) {
        if (o == null)
            return "";
        else
            return new String(((ByteBuffer) o).array());
    }

    @Deprecated
    @Override
    public String genDml() {
        StringBuilder sb = new StringBuilder();
        sb.append(getType().toString().toLowerCase());

        //type details
        int n = 0;
        if (getDelimiter() != null) {
            sb.append("(\"").append(AbUtils.encodeString(getDelimiter())).append("\"");
            n++;
        } else if (getLength() != null && getLength() > 0) {
            if (n > 0)
                sb.append(",");
            else
                sb.append("(");
            sb.append(getLength());
            n++;
        }
        if (n > 0)
            sb.append(")");

        //name and nvl
        sb.append(" ").append(getName());
        if (fixValue != null) {
            sb.append(" = \"").append(AbUtils.encodeString(fixValue.toString())).append("\"");
        } else if (nullable) {
            sb.append(" = NULL");
            if (nvl != null)
                if (nvl instanceof byte[])
                    sb.append("(\"").append(new String((byte[]) nvl)).append("\")");
                else
                    sb.append("(\"").append(nvl).append("\")");
        }

        return sb.append(";").toString();
    }


    @Override
    public Schema genColSchema(Boolean logicTypeSupport) {
        colSchema = Schema.create(AbUtils.abTypeToAvroTypeV2(type, getLength().intValue()));

        return colSchema;
    }

    @Override
    public void setNVL(String nvl) throws GdrException {
        if (nvl == null)
            this.nvl = null;
        else if (nvl.contains("\"")) {
            int s = StringUtils.indexOf(nvl, "\"");
            int l = StringUtils.lastIndexOf(nvl, "\"");
            if (l > s)
                nvl = nvl.substring(s + 1, l);
        }
        //TODO need validate from TD side
        if ("".equals(nvl)) {
            if (!hasBinaryHeader() && StringUtils.isBlank(this.delimiter)) {
                this.nvl = new byte[length.intValue()];
                for (int i = 0; i < nvl.length(); i++)
                    ((byte[]) this.nvl)[i] = 0;
            } else
                this.nvl = "".getBytes();
        } else
            try {
                this.nvl = convertValue(nvl.getBytes());
            } catch (GdrRuntimeException e) {
                //e.printStackTrace();
                this.nvl = nvl;
            }
    }
}
