package org.apache.gdr.common.util;

import org.apache.gdr.common.conf.Constant;
import org.apache.gdr.common.datatype.AbDataType;
import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.schema.column.*;
import org.apache.gdr.common.schema.column.typedetail.decimal.DecimalType;
import org.apache.gdr.common.schema.dataset.AbDataDef;
import org.apache.gdr.common.schema.enums.ColumnProp;
import com.google.gson.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.*;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map.Entry;

public class GdrParser {
    private static final Log LOG = LogFactory.getLog(GdrParser.class);

    public static void main(String[] args) throws IOException, GdrException {
        if (args.length != 3) {
            printUsage(args[0]);
        }

        BufferedReader br = new BufferedReader(new FileReader(args[1]));
        BufferedWriter bw = new BufferedWriter(new FileWriter(args[2]));
        StringBuilder sb = new StringBuilder();

        String sCurrentLine;

        while ((sCurrentLine = br.readLine()) != null) {
            sb.append(sCurrentLine).append("\n");
        }

        String name = args[1].replace(".dml", "");
        String[] strs = name.split("\\.", 3);
        if (strs.length > 1) {
            name = strs[1];
        } else {
            name = strs[0];
        }

        AbDataDef abSchema = GdrParser.parseGdr(sb.toString());
        LOG.debug(abSchema.getAvroSchema());
        bw.write(abSchema.getAvroSchema().toString());

        br.close();
        bw.close();
    }

    public static AbColumnDef parseDatetimeColumn(ObjectNode node) throws IOException, GdrException {
        AbColumnDef column = null;
        return column;
    }

    public static AbDataDef parseGdr(DataInputStream in) throws IOException, GdrException {
        StringBuilder sb = new StringBuilder();
        byte[] b = new byte[1024];
        while ((in.read(b)) > 0) {
            sb.append(new String(b));
        }
        return parseGdr(sb.toString());
    }

    public static AbDataDef parseGdr(String gdrFile) throws IOException, GdrException {
        AbDataDef dataset = new AbDataDef();
        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(new FileReader(gdrFile));
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        for (Entry<String, JsonElement> o : jsonObject.entrySet()) {
            populateTableProps(dataset, o);
        }

        return dataset;
    }

    static AbColumnDef parseNumericColumn(ObjectNode node) throws GdrException {
        AbColumnDef column = new AbNumberColumnDef();

        return column;
    }

    static AbColumnDef parseStringColumn(ObjectNode node) throws GdrException {
        return parseStringColumn(node, Constant.AB_DEFAULT_CHARSET);
    }

    static AbColumnDef parseStringColumn(ObjectNode node, Charset charset) throws GdrException {
        AbStringColumnDef column = new AbStringColumnDef();

        return column;
    }

    static protected AbColumnDef buildColumn(JsonObject obj) throws GdrException {
        JsonElement element = obj.get("type");
        AbColumnDef c = null;
        if (element instanceof JsonPrimitive) {
            c = AbColumnFactory.build(obj.get("type").getAsString());
        } else {
            Iterator<JsonElement> itr = ((JsonArray) element).iterator();
            while (itr.hasNext()) {
                String t = itr.next().getAsString();
                if (!t.equals("null")) {
                    c = AbColumnFactory.build(t);
                }
            }
        }

        return buildColumn(obj, c);
    }

    static protected AbColumnDef buildColumn(JsonObject obj, AbColumnDef c) throws GdrException {
        JsonElement element = obj.get("type");
        if (element instanceof JsonPrimitive) {
            c.setNullable(Boolean.FALSE);
        } else {
            Iterator<JsonElement> itr = ((JsonArray) element).iterator();
            boolean nullable = Boolean.FALSE;
            while (itr.hasNext()) {
                String t = itr.next().getAsString();
                if (t.equals("null")) {
                    nullable = Boolean.TRUE;
                } else {
                    //c = AbColumnFactory.build(t);
                }
            }
            c.setNullable(nullable);
        }

        for (Entry<String, JsonElement> o : obj.entrySet()) {
            if (ColumnProp.NAME.equals(o.getKey())) {
                c.setName(o.getValue().getAsString());
            } else if (ColumnProp.DELIMITER.equals(o.getKey())) {
                c.setDelimiter(AbUtils.decodeString(o.getValue().getAsString()));
            } else if (ColumnProp.LENGTH.equals(o.getKey())) {
                c.setLength(o.getValue().getAsLong());
            } else if (ColumnProp.BYTEHEAD.equals(o.getKey())) {
                c.setBinaryHeader(o.getValue().getAsBoolean());
            } else if (ColumnProp.ENABLEOUTPUT.equals(o.getKey())) {
                c.setOutput(o.getValue().getAsBoolean());
            } else if (ColumnProp.FIXVALUE.equals(o.getKey())) {
                c.setFixValue(o.getValue().getAsString());
            } else if (ColumnProp.HEADERENDIAN.equals(o.getKey())) {
                c.setHeaderEndian(o.getValue().getAsString().equals(ByteOrder.BIG_ENDIAN.toString())
                        ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            } else if (ColumnProp.NVL.equals(o.getKey())) {
                c.setNVL(o.getValue().getAsString());
            } else if (ColumnProp.UNSIGNHRADERLEN.equals(o.getKey())) {
                c.setUnsignHeaderLength(o.getValue().getAsBoolean());
            } else if (ColumnProp.SCALE.equals(o.getKey())) {
                ((AbDecimalColumnDef) c).setScale(o.getValue().getAsInt());
            } else if (ColumnProp.UNSIGN_VALUE.equals(o.getKey())) {
                ((AbNumberColumnDef) c).setUnsignValue(o.getValue().getAsBoolean());
            } else if (ColumnProp.VALUE_ENDIAN.equals(o.getKey())) {
                if (c instanceof AbNumberColumnDef)
                    ((AbNumberColumnDef) c)
                            .setValueEndian(o.getValue().getAsString().equals(ByteOrder.BIG_ENDIAN.toString())
                                    ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            } else if (ColumnProp.CHARSET.equals(o.getKey())) {
                ((AbStringColumnDef) c).setCharset(Charset.forName(o.getValue().getAsString()));
            } else if (ColumnProp.DATETIME_FORMAT.equals(o.getKey())) {
                if (c instanceof AbTimeStampColumnDef)
                    ((AbTimeStampColumnDef) c).setDatetimeFormat(o.getValue().getAsString());
                else if (c instanceof AbDateColumnDef)
                    ((AbDateColumnDef) c).setDatetimeFormat(o.getValue().getAsString());
            } else if (ColumnProp.DECIMAL_TYPE.equals(o.getKey())) {
                ((AbDecimalColumnDef) c).setDecimalType(DecimalType.fromString(o.getValue().getAsString()));
            }
        }
        return c;
    }

    static protected void populateTableProps(AbDataDef dataDef, Entry<String, JsonElement> entry)
            throws GdrException {
        switch (entry.getKey()) {
            case "name":
                dataDef.setName(entry.getValue().getAsString());
                break;
            case "namespace":
                // do nothing
                break;
            case "type":
                if (!"record".equalsIgnoreCase(entry.getValue().getAsString()))
                    throw new GdrRuntimeException("bad gdr file: type must be record.");
            case "hasHideNull":
                // do nothing
                break;
            case "fields":
                Iterator<JsonElement> itr = ((JsonArray) entry.getValue()).iterator();
                while (itr.hasNext()) {
                    JsonObject o = itr.next().getAsJsonObject();
                    AbColumnDef c = buildColumn(o);
                    if (c instanceof AbDecimalColumnDef
                            && (((AbDecimalColumnDef) c).getScale() == null
                            || c.getLength() <= 0l)) {
                        c = new AbNumberColumnDef();
                        c.setType(AbDataType.DECIMAL);
                        buildColumn(o, c);
                    }
                    dataDef.addColumn(c);
                }
        }
    }

    private static void printUsage(String jobName) {
        System.out.println(String.format("Usage: %s <input gdr file> <output avro schema>", jobName));
    }

    public static AbColumnDef readAbColumnDef(ObjectNode node) throws GdrException {
        return null;
    }
}
