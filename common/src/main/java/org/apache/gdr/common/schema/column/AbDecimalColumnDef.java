package org.apache.gdr.common.schema.column;

import org.apache.gdr.common.datatype.AbDataType;
import org.apache.gdr.common.exception.AbTypeException;
import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.schema.enums.ColumnProp;
import org.apache.gdr.common.util.AbUtils;
import org.apache.gdr.common.util.StreamSearcher;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.gdr.common.schema.column.typedetail.decimal.*;
import org.codehaus.jackson.node.ObjectNode;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;

public class AbDecimalColumnDef extends AbColumnDef {
    private static final Log LOG = LogFactory.getLog(AbDecimalColumnDef.class);
    private static final String DOUBLEFORMAT = "%.{n}f";
    private String outputFormat = null;
    private Integer scale;
    private StreamSearcher streamSearch;
    private Conversion<BigDecimal> conversion;
    private DecimalType decimalType;
    private BigDecimal scaleLen;

    public AbDecimalColumnDef() {
        super();
        conversion = new Conversions.DecimalConversion();
        scaleLen = BigDecimal.valueOf(1);
    }

    public void setDecimalType(DecimalType type) {
        decimalType = type;
        if (DecimalType.IMPLICIT.equals(decimalType))
            this.scaleLen = BigDecimal.valueOf((long) Math.pow(10, scale));
    }

    public Boolean isImplicitDecimal() {
        return DecimalType.IMPLICIT.equals(decimalType);
    }

    @Override
    public ByteBuffer convertValue(byte[] input) throws AbTypeException {
        return this.convertValue(new String(input, StandardCharsets.US_ASCII));
    }

    public ByteBuffer convertValue(String input) throws AbTypeException {
        if (StringUtils.isBlank(input) || (nvl != null && nvl.equals(input)))
            return null;

        switch (getType()) {
            case DECIMAL:
                BigDecimal value = new BigDecimal(input.trim()).divide(scaleLen);

                return conversion.toBytes(value, null, colSchema.getLogicalType());
            default:
                throw new AbTypeException("undefined data type:" + getType());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj))
            return Boolean.FALSE;

        AbDecimalColumnDef abDecimalColumnDef = (AbDecimalColumnDef) obj;
        if (scale == null) {
            if (abDecimalColumnDef.scale != null)
                return Boolean.FALSE;
        } else if (!scale.equals(abDecimalColumnDef.scale))
            return Boolean.FALSE;

        if (scaleLen != abDecimalColumnDef.scaleLen)
            return Boolean.FALSE;

        return Boolean.TRUE;
    }

    @Override
    public String format(Object data) {
        if (data == null) {
            return (nvl == null) ? "" : nvl.toString();
        } else {
            BigDecimal rawValue = conversion.fromBytes((ByteBuffer) data, null, colSchema.getLogicalType());
            if (isImplicitDecimal())
                rawValue = rawValue.multiply(scaleLen);
            DecimalFormat decimalFormat = null;
            String format = "";
            if (scale != null && scale > 0) {
                return String.format(outputFormat, rawValue);
            } else if ((rawValue.compareTo(BigDecimal.ONE) < 0 && (rawValue.compareTo(BigDecimal.ZERO) > 0))
                    || (rawValue.compareTo(BigDecimal.ZERO) < 0 && (rawValue.compareTo(new BigDecimal(-1)) > 0)))
                format = ".###";
            else
                format = "##";

            decimalFormat = new DecimalFormat(format);
            return decimalFormat.format(rawValue);
        }
    }

    private String format(Object data, Long length) {
        if (length == null || length <= 0)
            return format(data);

        StringBuilder stringBuilder = new StringBuilder();
        if (data == null) {
            for (int i = 0; i < length; i++)
                stringBuilder.append(" ");
            return stringBuilder.toString();
        } else {
            String format = format(data);
            for (int i = 0; i < length - format.length(); i++)
                stringBuilder.append(" ");
            stringBuilder.append(format);
            return stringBuilder.toString();
        }
    }

    @Override
    public Schema genColSchema(Boolean logicTypeSupport) {
        if (logicTypeSupport) {
            if (scale == null)
                colSchema = Schema.create(AbUtils.abTypeToAvroTypeV1(type, getLength().intValue(), scale));
            else
                colSchema = LogicalTypes.decimal(length.intValue(), scale)
                        .addToSchema(Schema.create(AbUtils.abTypeToAvroTypeV2(type, getLength().intValue())));
        } else
            colSchema = Schema.create(AbUtils.abTypeToAvroTypeV1(type, getLength().intValue(), scale));

        return colSchema;
    }

    String getDigitalStr(String input) {
        if (StringUtils.isBlank(input))
            return "";

        return StringUtils.trim(input).replaceAll("[\",']", "");
    }

    @Override
    public ObjectNode getGdrSchema() {
        ObjectNode node = super.getGdrSchema();
        if (scale != null)
            node.put(ColumnProp.SCALE.getName(), this.scale);
        if (decimalType != null)
            node.put(ColumnProp.DECIMAL_TYPE.getName(), decimalType.getName());

        return node;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;

        if (outputFormat == null) {
            outputFormat = DOUBLEFORMAT.replace("{n}", scale.toString());
        }
    }

    @Override
    public ByteBuffer read(InputStream dis, boolean isNull) throws GdrException, IOException {
        if (dis.available() <= 0)
            throw new GdrRuntimeException("unexpected EOF for column: " + this.getName());

        byte[] bytes;

        initBytesRead();

        if (type.equals(AbDataType.DECIMAL)) {
            if (delimiter == null) {
                bytes = new byte[length.intValue()];
                setBytesRead(dis.read(bytes));
                String strValue = new String(bytes);
                return (ByteBuffer) V2N(convertValue(strValue.trim()));
            } else {
                if ((bytes = streamSearch.search(dis)) != null) {
                    setBytesRead(bytes.length + delimiter.length());
                    if (!isNull) {
                        if (nvl != null)
                            if (nvl instanceof String
                                    && ((String) nvl).equals(new String(bytes, StandardCharsets.UTF_8)))
                                return null;
                            else if (nvl instanceof ByteBuffer) {
                                return (ByteBuffer) V2N(convertValue(bytes));
                            }
                        return (ByteBuffer) V2N(convertValue(bytes));
                    } else
                        return null;
                }

                throw new EOFException("unexpected EOF for column: " + this.getName());
            }
        } else
            throw new GdrRuntimeException("unsupported data type: " + type);

    }

    @Override
    public void setDelimiter(String delimiter) {
        super.setDelimiter(delimiter);
        streamSearch = new StreamSearcher(delimiter.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void setFixValue(String value) {
        try {
            fixValue = convertValue(getDigitalStr(value));
        } catch (AbTypeException e) {
            LOG.info(e.getLocalizedMessage());
            fixValue = null;
        }
    }

    @Override
    public void setNVL(String nvl) {
        try {
            String digitalStr = this.getDigitalStr(nvl);
            if ("".equals(digitalStr))
                this.nvl = digitalStr;
            else if (!StringUtils.isNumeric(digitalStr))
                this.nvl = digitalStr;
            else
                this.nvl = convertValue(digitalStr);
        } catch (AbTypeException e) {
            LOG.info(e.getLocalizedMessage());
            this.nvl = null;
        }
    }

    @Override
    public void setTypeDetail(String typeDef) {
        TypeDetailInterface handler = new DecimalDelimiterLengthScaleHandler();
        if (handler.match(typeDef)) {
            handler.parseTypeDetail(typeDef, this);
            return;
        }

        handler = new DecimalDelimiterLengthHandler();
        if (handler.match(typeDef)) {
            handler.parseTypeDetail(typeDef, this);
            return;
        }

        handler = new DecimalDelimiterScaleHandler();
        if (handler.match(typeDef)) {
            handler.parseTypeDetail(typeDef, this);
            return;
        }

        handler = new DecimalFixLengthScaleHandler();
        if (handler.match(typeDef)) {
            handler.parseTypeDetail(typeDef, this);
            return;
        }

        handler = new DecimalFixLengthHandler();
        if (handler.match(typeDef)) {
            handler.parseTypeDetail(typeDef, this);
            return;
        }

        handler = new DecimalDelimiterOnlyHandler();
        if (handler.match(typeDef)) {
            handler.parseTypeDetail(typeDef, this);
            return;
        }

        throw new GdrRuntimeException("unspported type definition: " + typeDef);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(super.toString());
        if (scale != null)
            stringBuilder.append(",")
                    .append(ColumnProp.SCALE.getName())
                    .append("=")
                    .append(scale);
        if (decimalType != null)
            stringBuilder.append(",")
                    .append(ColumnProp.DECIMAL_TYPE.getName())
                    .append("=")
                    .append(decimalType.getName());

        return stringBuilder.toString();
    }

    @Override
    public void write(OutputStream dos, Object value, Boolean defaultToNull) throws IOException {
        if (type.equals(AbDataType.DECIMAL)) {
            Object tmp = N2V(value);
            if (delimiter == null)
                dos.write(format(tmp, length).getBytes(StandardCharsets.US_ASCII));
            else {
                dos.write(format(tmp).getBytes(StandardCharsets.US_ASCII));
                dos.write(delimiter.getBytes(StandardCharsets.UTF_8));
            }
        } else
            throw new GdrRuntimeException("unsupported data type: " + type);
    }
}
