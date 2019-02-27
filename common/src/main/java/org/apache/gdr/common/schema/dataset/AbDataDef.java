package org.apache.gdr.common.schema.dataset;

import org.apache.gdr.common.AbRecord;
import org.apache.gdr.common.conf.ConfigureInterface;
import org.apache.gdr.common.conf.Constant;
import org.apache.gdr.common.conf.GdrConfigure;
import org.apache.gdr.common.exception.AbTypeException;
import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.schema.column.AbColumnDef;
import org.apache.gdr.common.schema.enums.TableProp;
import org.apache.gdr.common.util.AbUtils;
import org.apache.gdr.common.util.DmlParser;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.*;

public class AbDataDef {
    public final static String DUMMYNAME = "dummy";
    static Log LOG = LogFactory.getLog(AbDataDef.class);
    String name;
    Charset charset;
    Boolean hasHideNull;
    List<AbColumnDef> columnList;
    Map<String, Integer> columnLkp;
    Map<String, Integer> hideNullColumnLkp;
    Schema schema;
    String comment;
    String outputDel;
    String dmlStr;
    int headerLen, hideNullCol;
    private ConfigureInterface conf = GdrConfigure.create();

    public AbDataDef() {
        init();
    }

    public AbDataDef(String name) {
        this();
        this.name = name;
    }

    public ConfigureInterface getConf() {
        return conf;
    }

    public void setConf(ConfigureInterface conf) {
        this.conf = conf;
    }

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public Map<String, Integer> getHideNullColumnLkp() {
        return hideNullColumnLkp;
    }

    public void setHideNullColumnLkp(Map<String, Integer> hideNullColumnLkp) {
        this.hideNullColumnLkp = hideNullColumnLkp;
    }

    public Integer indexOfHideNullColumn(String name) {
        return hideNullColumnLkp.get(name);
    }

    public void init() {
        hasHideNull = Boolean.FALSE;
        headerLen = hideNullCol = 0;
        outputDel = Constant.DSS_DEFAULT_COL_DELIMITER;
        initColumnList();
        initColumnLkp(Boolean.TRUE);
        initHideNullColumnLkp(Boolean.TRUE);
    }

    void initColumnLkp(boolean force) {
        if (force || columnLkp == null || columnLkp.size() < 1) {
            columnLkp = new HashMap<>();
            int i = 0;
            for (AbColumnDef col : getColumnList()) {
                columnLkp.put(col.getName(), i++);
            }
        }
    }

    void initHideNullColumnLkp(boolean force) {
        if (force || hideNullColumnLkp == null || hideNullColumnLkp.size() < 1) {
            hideNullColumnLkp = new HashMap<>();
            int i = 0;
            for (AbColumnDef col : getColumnList()) {
                if (col.isHideNull())
                    hideNullColumnLkp.put(col.getName(), i++);
            }
        }
    }

    public void initColumnList() {
        this.columnList = new ArrayList<>();
    }

    public Map<String, Integer> getColumnLkp() {
        return columnLkp;
    }

    public void setColumnLkp(Map<String, Integer> columnLkp) {
        this.columnLkp = columnLkp;
    }

    public Integer getColumnIndex(String colName) {
        return columnLkp.get(colName);
    }

    public AbColumnDef getColumnDef(String colName) {
        return getColumnDef(getColumnIndex(colName));
    }

    public AbColumnDef getColumnDef(int i) {
        return columnList.get(i);
    }

    public void addColumn(AbColumnDef c) throws AbTypeException {
        this.columnList.add(c);
        if (c.isNullable() && c.getNVL() == null) {
            hideNullCol++;
            this.headerLen = genHeaderLen(hideNullCol);
            hasHideNull = Boolean.TRUE;
        }
        initColumnLkp(Boolean.TRUE);
        initHideNullColumnLkp(Boolean.TRUE);
    }

    int genHeaderLen(int hideNullCol) {
        return (hideNullCol / 8) + (hideNullCol % 8 == 0 ? 0 : 1);
    }

    public void addColumns(Collection<AbColumnDef> cs) throws AbTypeException {
        for (AbColumnDef c : cs) {
            addColumn(c);
        }
    }

    public List<AbColumnDef> getColumnList() {
        return columnList;
    }

    public AbColumnDef getColumn(int i) {
        return columnList.get(i);
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getDmlStr() {
        if (dmlStr == null)
            dmlStr = genDml();
        return dmlStr;
    }

    public void setDmlStr(String dmlStr) {
        this.dmlStr = dmlStr;
    }

    protected String genDml() {
        StringBuilder sb = new StringBuilder();
        //global charset for table
        if (charset != null)
            sb.append(charset.toString());

        //dml start
        sb.append(DmlParser.DmlHead.toLowerCase()).append("\n");

        //append column dml
        for (AbColumnDef c : columnList) {
            sb.append("  ").append(c.genDml()).append("\n");
        }
        //add dml end
        sb.append(DmlParser.DmlEnd.toLowerCase()).append("\n");

        return sb.toString();
    }

	/*
	public AbRecord readRecord(InputStream dis, AbRecord record) throws IOException, GdrException {
		if (record.read(dis))
			return record;
		else
			return null;
	}
	
	public GenericRecord readRecordInAvro(InputStream dis, Boolean ignoreDummyCol) throws IOException, GdrException {
		AbRecord r = readRecord(dis);
		if (r != null) 
			return r.toGenericRecord(ignoreDummyCol, Boolean.TRUE);
		else 
			return null;
	}
	*/

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name.toLowerCase();
    }
/*
	public String readRecordInString(InputStream dis, Boolean ignoreDummyCol) throws IOException, GdrException {
		AbRecord r = readRecord(dis);
		if (r != null) 
			return r.toString(ignoreDummyCol);
		else 
			return null;
	}

	public String readRecordInString(InputStream dis, Boolean ignoreDummyCol, AbRecord r) throws IOException, GdrException {
		readRecord(dis, r);
		if (r != null) 
			return r.toString(ignoreDummyCol);
		else 
			return null;
	}
*/

    public String getOutputDel() {
        return outputDel;
    }

    public void setOutputDel(String outputDel) {
        this.outputDel = outputDel;
    }

    public AbRecord readRecord(InputStream dis) throws IOException, GdrException {
        AbRecord record = new AbRecord(this);
        if (record.read(dis))
            return record;
        else
            return null;
    }

    public AbRecord readRecordWithoutDummyColumn(InputStream dis) throws IOException, GdrException {
        AbRecord record = new AbRecord(this, Boolean.TRUE);
        if (record.read(dis))
            return record;
        else
            return null;
    }

    public Boolean hasHideNull() {
        return hasHideNull;
    }

    public void remoeveColumn(AbColumnDef c) {
        this.columnList.remove(c);
        initColumnLkp(Boolean.TRUE);
        initHideNullColumnLkp(Boolean.TRUE);
    }

    public void remoeveColumns(Collection<AbColumnDef> c) {
        this.columnList.removeAll(c);
        initColumnLkp(Boolean.TRUE);
        initHideNullColumnLkp(Boolean.TRUE);
    }

    public Schema getAvroSchema() throws AbTypeException {
        if (this.schema != null)
            return schema;

        return genAvroSchema(Boolean.FALSE, Boolean.TRUE);
    }

    public int getHeaderLen() {
        return this.headerLen;
    }

    public AbDataDef copy(Boolean skipDummyCol) throws AbTypeException {
        AbDataDef d = new AbDataDef();
        d.setDmlStr(this.getDmlStr());
        d.setComment(this.getComment());
        List<AbColumnDef> l = new ArrayList<>();
        for (AbColumnDef c : this.getColumnList())
            if (!skipDummyCol || c.enableOutput())
                l.add(c);
        d.addColumns(l);
        return d;
    }

    public Schema genAvroSchema(Boolean skipDummyCol) throws AbTypeException {
        return genAvroSchema(skipDummyCol, Boolean.FALSE);
    }

    /**
     * @param skipDummyCol
     * @param logicTypeSupport(for avro 1.8.0 and above,long can be defined as timestamp
     *and int can be defined as date
     * @return
     * @throws AbTypeException
     */
    public Schema genAvroSchema(Boolean skipDummyCol, Boolean logicTypeSupport) throws AbTypeException {
        Schema schema = Schema.createRecord(AbUtils.removeIllegalAvroChar(name), "doc", "com.ebay.dss.adpo.ab", Boolean.FALSE);
        List<Field> fields = new ArrayList<>();

        for (AbColumnDef col : this.columnList) {
            if (!skipDummyCol || col.enableOutput()) {
                if (col.isNullable()) {
                    List<Schema> types = new ArrayList<>();
                    types.add(Schema.create(Type.NULL));
                    types.add(col.genColSchema(logicTypeSupport));
                    Schema unionSchema = Schema.createUnion(types);
                    fields.add(new Field(col.getName(), unionSchema, null, col.getNVL() == null ? JsonProperties.NULL_VALUE : col.getNVL()));
                } else {
                    fields.add(new Field(col.getName(), col.genColSchema(logicTypeSupport), null, JsonProperties.NULL_VALUE));
                }
            }
        }
        schema.setFields(fields);

        this.schema = schema;

        return schema;
    }

    public ObjectNode getGdrSchema() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode gen = mapper.createObjectNode();
        try {
            gen.put("type", "record");
            gen.put(TableProp.NAME.getName(), name);
            gen.put(TableProp.NAMESPACE.getName(), Constant.DSS_DEFAULT_SCHEMA_NAMESPACE);
            gen.put(TableProp.HIDENULL.getName(), this.hasHideNull);
            fieldsToJson(gen);
            return gen;
        } catch (IOException e) {
            e.printStackTrace();
            throw new GdrRuntimeException(e.getMessage());
        }
    }

    protected ObjectNode fieldsToJson(ObjectNode gen) throws IOException {
        ArrayNode array = gen.putArray(TableProp.FIELDS.getName());
        for (AbColumnDef col : this.columnList) {
            array.add(col.getGdrSchema());
        }
        return gen;
    }

    @Override
    public String toString() {
        return getGdrSchema().toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AbDataDef))
            return Boolean.FALSE;
        AbDataDef other = (AbDataDef) obj;
        if (!getName().equals(other.getName()))
            return Boolean.FALSE;
        if (!hasHideNull().equals(other.hasHideNull()))
            return Boolean.FALSE;
        if (columnList.size() != other.getColumnList().size())
            return Boolean.FALSE;
        for (int i = 0; i < columnList.size(); i++)
            if (!getColumn(i).equals(other.getColumn(i)))
                return Boolean.FALSE;

        return Boolean.TRUE;
    }
}
