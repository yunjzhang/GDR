package org.apache.gdr.common.schema.column;

import org.apache.gdr.common.conf.ConfigureInterface;
import org.apache.gdr.common.conf.Constant;
import org.apache.gdr.common.datatype.AbDataType;
import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.schema.enums.ColumnProp;
import org.apache.gdr.common.util.AbUtils;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.List;

public abstract class AbColumnDef {
    static final Log LOG = LogFactory.getLog(AbColumnDef.class);
    protected String name;
    protected AbDataType type;
    protected Long length;
    protected Boolean nullable;
    protected Object nvl;
    protected Boolean byteHeader;
    protected Boolean unsignHeaderLength;
    protected ByteOrder headerEndian;
    protected String delimiter;
    protected Object fixValue;
    protected String comment;
    protected Boolean output;
    protected long bytesRead;
    protected Schema colSchema;
    protected Boolean isCompound = Boolean.FALSE;
    protected List<AbColumnDef> fields = null;
    private ConfigureInterface conf;

    public AbColumnDef() {
        init();
    }

    public abstract Schema genColSchema(Boolean logicTypeSupport);

    public Schema getColSchema() {
        if (colSchema == null)
            return genColSchema(Boolean.TRUE);
        else
            return colSchema;
    }

    public void setColSchema(Schema colSchema) {
        this.colSchema = colSchema;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(long bytesRead) throws GdrException {
        this.bytesRead = bytesRead;
    }

    public void initBytesRead() throws GdrException {
        this.bytesRead = 0l;
    }

    public Boolean validateLegth(long bytesRead) throws GdrException {
        if (length != null && length != 0
                && length != bytesRead) {
            return Boolean.FALSE;
        } else {
            return Boolean.TRUE;
        }

    }

    void init() {
        length = -1l;
        nullable = Boolean.TRUE;
        byteHeader = Boolean.FALSE;
        unsignHeaderLength = Boolean.TRUE;
        output = Boolean.TRUE;
        headerEndian = Constant.DSS_DEFAULT_ENDIAN;
    }

    public abstract Object convertValue(byte[] input) throws GdrException;

    public Boolean enableOutput() {
        return output;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Long getLength() {
        return length;
    }

    public void setLength(Long length) {
        this.length = length;
    }

    public Object getFixValue() {
        return fixValue;
    }

    public void setFixValue(String fixValue) {
        try {
            this.fixValue = convertValue(fixValue.getBytes());
        } catch (GdrException e) {
            e.printStackTrace();
            fixValue = null;
        }
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String readAsString(DataInputStream dis, boolean isNull) throws IOException, GdrException {
        Object o = read(dis, isNull);
        if (o == null)
            return "";
        else
            return o.toString();
    }

    public AbDataType getType() {
        return type;
    }

    public void setType(AbDataType type) {
        this.type = type;
    }

    public String getValueString(Object o) {
        if (o != null)
            return o.toString();
        else
            return null;
    }

    public Boolean hasBinaryHeader() {
        return byteHeader;
    }

    public Boolean isNullable() {
        return nullable;
    }

    public Boolean isUnsignHeaderLength() {
        return unsignHeaderLength;
    }

    public abstract Object read(InputStream dis, boolean isNull) throws GdrException, IOException;

    public void setBinaryHeader(Boolean isBinary) {
        this.byteHeader = isBinary;
    }

    public void setHeaderEndian(ByteOrder endian) {
        this.headerEndian = endian;
    }

    public void setNameAndDefault(String subStr) throws GdrException {
        // get column name
        String[] array = subStr.replace(";", "").split("=");
        setName(array[0].trim().toLowerCase());
        setNullable(Boolean.FALSE);

        // get default value
        String tmpStr = null;
        if (array.length > 1) {
            tmpStr = array[1].trim();
            if (tmpStr.contains("NULL")) {
                setNullable(Boolean.TRUE);
                int p0 = tmpStr.indexOf("(") + 1;
                int p1 = tmpStr.indexOf(")");
                if (p1 > p0)
                    setNVL(AbUtils.decodeString(AbUtils.getQuoteString(tmpStr.substring(p0, p1))));
            } else {
                String dValue = AbUtils.decodeString(AbUtils.getQuoteString(array[1].trim()));
                if (StringUtils.isNotBlank(dValue)) {
                    if (this.delimiter == null && dValue.length() != length) {
                        throw new GdrRuntimeException("ERROR: unable to set default value for column:" + getName());
                    } else {
                        setFixValue(dValue);
                    }
                }
            }
        }

        if (StringUtils.startsWithAny(getName(), Constant.IGNORE_COLUMN_NAME)) {
            setOutput(Boolean.FALSE);
        }
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    public void setOutput(Boolean output) {
        this.output = output;
    }

    public void setTypeDetail(String typeDef) {
        setBinaryHeader(Boolean.FALSE);
        if (StringUtils.isNumeric(typeDef)) {
            // fix length without header
            setLength(Long.valueOf(typeDef));
        } else if (typeDef.trim().startsWith("\"") || typeDef.trim().startsWith("U\"")) {
            // delimiter "\007"
            typeDef = StringUtils.substringBetween(typeDef, "\"");
            setDelimiter(AbUtils.decodeString(typeDef));
        } else if (typeDef.trim().startsWith("\'") || typeDef.trim().startsWith("U\'")) {
            // delimiter '\007'
            typeDef = StringUtils.substringBetween(typeDef, "\'");
            setDelimiter(AbUtils.decodeString(typeDef));
        } else if (StringUtils.containsIgnoreCase(typeDef, "INTEGER")) {
            // dynamic length with header
            setBinaryHeader(Boolean.TRUE);
            if (StringUtils.containsIgnoreCase(typeDef, "unsigned")) {
                setUnsignHeaderLength(Boolean.TRUE);
            } else {
                setUnsignHeaderLength(Boolean.FALSE);
            }
            if (StringUtils.containsIgnoreCase(typeDef, "big endian")) {
                setHeaderEndian(ByteOrder.BIG_ENDIAN);
            } else {
                setHeaderEndian(ByteOrder.LITTLE_ENDIAN);
            }

            Long length = Long.valueOf(StringUtils.substringBetween(typeDef, "(", ")"));
            setLength(length);
        }
    }

    public void setUnsignHeaderLength(Boolean unsign) {
        this.unsignHeaderLength = unsign;
    }

    @Override
    public String toString() {
        return getGdrSchema().toString();
    }

    public Object getNVL() {
        return nvl;
    }

    public void setNVL(String nvl) throws GdrException {
        if (nvl == null)
            this.nvl = null;
        else if (nvl.contains("\"")) {
            int s = StringUtils.indexOf(nvl, "\"");
            int l = StringUtils.lastIndexOf(nvl, "\"");
            if (l > s)
                nvl = nvl.substring(s + 1, l);
        }

        if ("".equals(nvl)) {
            if (!hasBinaryHeader() && StringUtils.isBlank(this.delimiter))
                this.nvl = String.format("%1$" + length.intValue() + "s", nvl);
            else
                this.nvl = "";
        } else
            try {
                this.nvl = convertValue(nvl.getBytes());
            } catch (GdrRuntimeException e) {
                //e.printStackTrace();
                this.nvl = nvl;
            }
    }

    public boolean isNVL(Object v) {
        return v != null && v.equals(nvl);
    }

    public Object N2V(Object v) {
        return v == null ? nvl : v;
    }

    public Object V2N(Object v) {
        return nvl != null && nvl.equals(v) ? null : v;
    }

    public abstract void write(OutputStream dos, Object value, Boolean defaultToNull) throws GdrException, IOException;

    public void write(OutputStream dos, Object value) throws GdrException, IOException {
        write(dos, value, Boolean.FALSE);
    }

    public Boolean isHideNull() {
        return this.isNullable() && this.getNVL() == null;
    }

    public String format(Object o) {
        if (o == null)
            return "";
        else
            return o.toString();
    }

    public ObjectNode getGdrSchema() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode gen = mapper.createObjectNode();

        gen.put("name", this.getName());

        if (isNullable()) {
            ArrayNode array = gen.putArray("type");
            array.add("null");
            array.add(this.type.getName());
        } else {
            gen.put("type", this.type.getName());
        }

        if (getFixValue() != null) {
            gen.put("default", getFixValue().toString());
        }

        if (delimiter != null) {
            gen.put(ColumnProp.DELIMITER.getName(), AbUtils.encodeString(delimiter));
        } else {
            gen.put(ColumnProp.BYTEHEAD.getName(), byteHeader);
            if (unsignHeaderLength)
                gen.put(ColumnProp.UNSIGNHRADERLEN.getName(), unsignHeaderLength);
            if (headerEndian != null
                    && !ByteOrder.LITTLE_ENDIAN.equals(headerEndian))
                gen.put(ColumnProp.HEADERENDIAN.getName(), headerEndian.toString());
        }
        if (!output)
            gen.put(ColumnProp.ENABLEOUTPUT.getName(), output);
        if (fixValue != null)
            gen.put(ColumnProp.FIXVALUE.getName(), fixValue.toString());
        if (length != null && length > 0)
            gen.put(ColumnProp.LENGTH.getName(), length);
        if (nvl != null)
            gen.put(ColumnProp.NVL.getName(), nvl.toString());

        return gen;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AbColumnDef))
            return Boolean.FALSE;

        AbColumnDef c = (AbColumnDef) obj;
        if (!length.equals(c.length))
            return Boolean.FALSE;
        if (!name.equals(c.name))
            return Boolean.FALSE;
        if (!type.equals(c.type))
            return Boolean.FALSE;
        if (!nullable.equals(c.nullable))
            return Boolean.FALSE;
        if (!byteHeader.equals(c.byteHeader))
            return Boolean.FALSE;
        if (!output.equals(c.output))
            return Boolean.FALSE;
        if (!headerEndian.equals(c.headerEndian))
            return Boolean.FALSE;
        if (!unsignHeaderLength.equals(c.unsignHeaderLength))
            return Boolean.FALSE;
        if (delimiter == null) {
            if (c.delimiter != null)
                return Boolean.FALSE;
        } else if (!delimiter.equals(c.delimiter))
            return Boolean.FALSE;
        if (fixValue == null) {
            if (c.fixValue != null)
                return Boolean.FALSE;
        } else if (!fixValue.equals(c.fixValue))
            return Boolean.FALSE;
        if (nvl == null) {
            if (c.nvl != null)
                return Boolean.FALSE;
        } else if (!nvl.equals(c.nvl))
            return Boolean.FALSE;

        return Boolean.TRUE;
    }

    @Deprecated
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
                sb.append("(\"").append(nvl).append("\")");
        }

        return sb.append(";").toString();
    }
}
