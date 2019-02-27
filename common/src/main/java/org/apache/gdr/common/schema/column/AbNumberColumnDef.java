package org.apache.gdr.common.schema.column;

import org.apache.gdr.common.conf.Constant;
import org.apache.gdr.common.datatype.AbDataType;
import org.apache.gdr.common.exception.AbTypeException;
import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.schema.enums.ColumnProp;
import org.apache.gdr.common.util.AbUtils;
import org.apache.gdr.common.util.StreamSearcher;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.DecimalFormat;

public class AbNumberColumnDef extends AbColumnDef {
    static final Log LOG = LogFactory.getLog(AbNumberColumnDef.class);
    static final String DOUBLEFORMAT = "%.{n}f";
    String outputFormat = null;
    Integer scale;
    ByteOrder valueEndian;
    Boolean unsignValue;
    StreamSearcher streamSearch;

    public AbNumberColumnDef() {
        super();
        valueEndian = Constant.DSS_DEFAULT_ENDIAN;
        unsignValue = Boolean.FALSE;
    }

    @Override
    public void setTypeDetail(String typeDef) {
        if (getType().equals(AbDataType.DECIMAL)) {
            // split column definition details by "," which is not in quotes
            String[] typeList = typeDef.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            setBinaryHeader(Boolean.FALSE);

            // set length
            String typePar = typeList[0].trim();
            if (StringUtils.isNumeric(typePar)) { //length as integer
                // fix length without header
                setLength(Long.valueOf(typePar));
            } else if (typePar.trim().startsWith("\"") || typePar.trim().startsWith("U\"")) {
                // delimiter "\007"
                typePar = StringUtils.substringBetween(typePar, "\"");
                setDelimiter(AbUtils.decodeString(typePar));
            } else if (typePar.trim().startsWith("\'") || typePar.trim().startsWith("U\'")) {
                // delimiter '\007'
                typePar = StringUtils.substringBetween(typePar, "\'");
                setDelimiter(AbUtils.decodeString(typePar));
            } else if (StringUtils.isNumeric(typePar.replace(".", ""))) { //40.2
                setLength(Long.valueOf(StringUtils.substringBefore(typePar, ".")));
                setPrecision(Integer.valueOf(StringUtils.substringAfterLast(typePar, ".")));
            }

            // set precision
            try {
                if (scale == null && typeList.length > 1 && StringUtils.isNotBlank(typeList[1])) {
                    setPrecision(Integer.valueOf(typeList[1].trim()));
                }
            } catch (Exception e) {
                LOG.debug("incompatible precision, ignored. [" + typeList[1] + "]");
            }
        } else {
            super.setTypeDetail(typeDef);
        }
    }

    @Override
    public void setDelimiter(String delimiter) {
        super.setDelimiter(delimiter);
        streamSearch = new StreamSearcher(delimiter.getBytes());
    }

    @Override
    public Number convertValue(byte[] input) throws AbTypeException {
        ByteBuffer wrapped = ByteBuffer.wrap(input).order(valueEndian);
        switch (getType()) {
            case INTEGER:
                if (unsignValue) {
                    if (length <= 4) {
                        return AbUtils.bytesToUnsignInt(input, valueEndian);
                    } else {
                        return AbUtils.bytesToLong(input, valueEndian);
                    }
                } else {
                    if (length == 4 || length == 3) {
                        return wrapped.getInt();
                    } else if (length == 2) {
                        return (int) wrapped.getShort();
                    } else if (length == 1) {
                        return (int) input[0];
                    } else {
                        return wrapped.getLong();
                    }
                }
            case DECIMAL:
                Double d = wrapped.getDouble();
                return d;
            case REAL:
                Double l = wrapped.getDouble();
                return l;
            default:
                throw new AbTypeException("undefined data type:" + getType());
        }
    }

    public Number convertValue(String input) throws AbTypeException {
        if (StringUtils.isBlank(input)
                || (nvl != null && nvl.equals(input)))
            return null;

        switch (getType()) {
            case INTEGER:
                Integer i = Integer.parseInt(input.trim());
                return i;
            case DECIMAL:
                if (getScale() != null && getScale() == 0) {
                    Long l = Long.parseLong(input.trim());
                    return l;
                } else {
                    Double d = Double.parseDouble(input.trim());
                    return d;
                }
            case REAL:
                Long l = Long.parseLong(input.trim());
                return l;
            default:
                throw new AbTypeException("undefined data type:" + getType());
        }
    }

    public Integer getScale() {
        return scale;
    }

    public Boolean getUnsignValue() {
        return unsignValue;
    }

    public void setUnsignValue(Boolean unsignValue) {
        this.unsignValue = unsignValue;
    }

    public ByteOrder getValueEndian() {
        return valueEndian;
    }

    public void setValueEndian(ByteOrder valueEndian) {
        this.valueEndian = valueEndian;
    }

    @Override
    public Number read(InputStream dis, boolean isNull) throws GdrException, IOException {
        if (dis.available() <= 0)
            throw new GdrRuntimeException("unexpected EOF for column: " + this.getName());

        byte[] bytes;

        initBytesRead();

        if (type.equals(AbDataType.DECIMAL)) {
            if (delimiter == null) {
                bytes = new byte[length.intValue()];
                setBytesRead(dis.read(bytes));
                String strValue = new String(bytes);
                return (Number) V2N(convertValue(strValue.trim()));
            } else {
                if ((bytes = streamSearch.search(dis)) != null) {
                    setBytesRead(bytes.length + delimiter.length());
                    if (!isNull) {
                        if (nvl != null)
                            if (nvl instanceof String
                                    && ((String) nvl).equals(new String(bytes)))
                                return null;
                            else if (nvl instanceof Number) {
                                return (Number) V2N(convertValue(new String(bytes)));
                            }
                        return (Number) V2N(convertValue(new String(bytes)));
                    } else
                        return null;
                }

                throw new EOFException("unexpected EOF for column: " + this.getName());
            }
        } else if (type.equals(AbDataType.INTEGER) || type.equals(AbDataType.REAL)) {
            bytes = new byte[length.intValue()];
            setBytesRead(dis.read(bytes));
            if (delimiter != null) {
                bytes = new byte[delimiter.length()];
                setBytesRead(getBytesRead() + dis.read(bytes));
            }

            if (!isNull) {
                if (nvl != null)
                    if (nvl instanceof String
                            && nvl.equals(new String(bytes)))
                        return null;
                    else if (nvl instanceof Number) {
                        return (Number) V2N(convertValue(bytes));
                    }
                return convertValue(bytes);
            } else {
                return null;
            }
        } else
            throw new GdrRuntimeException("unsupported data type: " + type);
    }

    @Override
    public void setFixValue(String value) {
        try {
            fixValue = convertValue(getDigitalStr(value));
        } catch (AbTypeException e) {
            e.printStackTrace();
            fixValue = null;
        }
    }

    @Override
    public void setNVL(String nvl) {
        try {
            String s = getDigitalStr(nvl);
            if ("".equals(s))
                this.nvl = s;
            else if (!NumberUtils.isNumber(s))
                this.nvl = s;
            else
                this.nvl = convertValue(s);
        } catch (AbTypeException e) {
            e.printStackTrace();
            this.nvl = null;
        }
    }

    public void setPrecision(Integer scale) {
        this.scale = scale;

        if (outputFormat == null) {
            outputFormat = DOUBLEFORMAT.replace("{n}", scale.toString());
        }
    }

    public String toString() {
        if (scale != null) {
            return super.toString() + ",precision=" + scale;
        } else {
            return super.toString();
        }
    }

    @Override
    public void write(OutputStream dos, Object value, Boolean defaultToNull) throws IOException {
        if (type.equals(AbDataType.DECIMAL)) {
            if (delimiter == null)
                dos.write(format(value, length).getBytes());
            else {
                dos.write(format(value).getBytes());
                dos.write(delimiter.getBytes());
            }
        } else if (type.equals(AbDataType.INTEGER))
            if (length <= 4) {
                Object tmp = N2V(value);
                if (tmp == null) {
                    tmp = Integer.valueOf(0);
                }
                dos.write(AbUtils.intToBytes((Integer) tmp, length.intValue(), valueEndian));
            } else {
                Object tmp = N2V(value);
                if (tmp == null) {
                    if (tmp == null) {
                        tmp = Long.valueOf(0);
                    }
                }
                dos.write(AbUtils.longToBytes((Long) tmp, length.intValue(), valueEndian));
            }
        else if (type.equals(AbDataType.REAL)) {
            Object tmp = N2V(value);
            if (tmp == null)
                if (this.nvl == null) {
                    tmp = Double.valueOf(0.0);
                }
            dos.write(AbUtils.doubleToBytes((Double) tmp, length.intValue(), valueEndian));
        } else
            throw new GdrRuntimeException("unsupported data type: " + type);
    }

    private String format(Object o, Long length) {
        if (length == null || length <= 0)
            return format(o);

        StringBuilder sb = new StringBuilder();
        if (o == null) {
            for (int i = 0; i < length; i++)
                sb.append(" ");
            return sb.toString();
        } else {
            String format = format(o);
            for (int i = 0; i < length - format.length(); i++)
                sb.append(" ");
            sb.append(format);
            return sb.toString();
        }
    }

    String getDigitalStr(String input) {
        if (StringUtils.isBlank(input))
            return "";

        return StringUtils.trim(input).replaceAll("[\",']", "");
    }

    @Override
    public String format(Object o) {
        if (o == null) {
            return (nvl == null) ? "" : nvl.toString();
        } else {
            DecimalFormat df = null;
            String format = "";
            BigDecimal n = new BigDecimal(o.toString());
            if (scale != null && scale > 0) {
                format = outputFormat;
                return String.format(outputFormat, n).replaceFirst("^0\\.", " .");
            } else if ((n.compareTo(new BigDecimal(1)) < 0 && (n.compareTo(new BigDecimal(0)) > 0))
                    || (n.compareTo(new BigDecimal(0)) < 0 && (n.compareTo(new BigDecimal(-1)) > 0))
                    || (o instanceof Double && ((Double) o) % 1 != 0) || (o instanceof Float && ((Float) o) % 1 != 0))
                format = ".###";
            else
                format = "##";

            df = new DecimalFormat(format);
            return df.format(n);
            // return String.format(format, n);
        }
    }

    @Override
    public ObjectNode getGdrSchema() {
        ObjectNode node = super.getGdrSchema();
        if (scale != null && scale > 0)
            node.put(ColumnProp.SCALE.getName(), this.scale);
        if (unsignValue)
            node.put(ColumnProp.UNSIGN_VALUE.getName(), this.unsignValue);
        if (valueEndian != null
                && !ByteOrder.LITTLE_ENDIAN.equals(valueEndian)) {
            node.put(ColumnProp.VALUE_ENDIAN.getName(), this.valueEndian.toString());
        }
        return node;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj))
            return Boolean.FALSE;

        AbNumberColumnDef c = (AbNumberColumnDef) obj;
        if (scale == null) {
            if (c.scale != null)
                return Boolean.FALSE;
        } else if (!scale.equals(c.scale))
            return Boolean.FALSE;

        if (valueEndian == null)
            return c.valueEndian == null;
        else
            return valueEndian.equals(c.valueEndian);
    }

    @Override
    public Schema genColSchema(Boolean logicTypeSupport) {
        colSchema = Schema.create(AbUtils.abTypeToAvroTypeV1(type, length.intValue(), scale));
        return colSchema;
    }
}
