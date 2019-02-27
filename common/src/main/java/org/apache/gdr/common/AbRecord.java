package org.apache.gdr.common;

import org.apache.gdr.common.conf.Constant;
import org.apache.gdr.common.exception.AbTypeException;
import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.schema.column.AbColumnDef;
import org.apache.gdr.common.schema.dataset.AbDataDef;
import org.apache.gdr.common.util.AbUtils;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;

public class AbRecord implements Serializable {
    private static final long serialVersionUID = 6213458797111084820L;
    private static final Log LOG = LogFactory.getLog(AbRecord.class);

    private AbDataDef def;
    private byte[] header;
    private GenericRecord data;
    private long bytesRead;

    public AbRecord(AbDataDef def) throws AbTypeException {
        this(def, Boolean.FALSE);
    }

    public AbRecord(AbDataDef def, Boolean skipDummyCol) throws AbTypeException {
        this.def = def.copy(skipDummyCol);

        header = new byte[def.getHeaderLen()];
        data = new GenericData.Record(def.genAvroSchema(Boolean.FALSE, Boolean.TRUE));
    }

    @Override
    public AbRecord clone() throws CloneNotSupportedException {
        super.clone();
        try {
            return clone(Boolean.FALSE);
        } catch (AbTypeException e) {
            LOG.debug(e.getLocalizedMessage());
            throw new GdrRuntimeException(e.getMessage());
        }
    }

    public AbRecord clone(Boolean skipDummyCol) throws AbTypeException {
        AbRecord newRecord = new AbRecord(def.copy(skipDummyCol));
        newRecord.setData(cloneData(skipDummyCol));
        newRecord.setHeader(cloneHeader(skipDummyCol, newRecord.getData()));
        return newRecord;
    }

    protected GenericRecord cloneData(Boolean skipDummyCol) throws AbTypeException {
        GenericRecord nData = new GenericData.Record(def.genAvroSchema(skipDummyCol));
        for (AbColumnDef col : def.getColumnList()) {
            if (!skipDummyCol || col.enableOutput())
                nData.put(col.getName(), data.get(col.getName()));
        }
        return nData;
    }

    private GenericRecord cloneData(GenericRecord data, boolean skipDummyCol) {
        GenericRecord nData = new GenericData.Record(data.getSchema());
        for (AbColumnDef col : def.getColumnList()) {
            if (!skipDummyCol || col.enableOutput())
                nData.put(col.getName(), data.get(col.getName()));
        }
        return nData;
    }

    protected byte[] cloneHeader(Boolean skipDummyCol, GenericRecord data) throws AbTypeException {
        if (!skipDummyCol) {
            byte[] nHeader = new byte[header.length];
            System.arraycopy(header, 0, nHeader, 0, header.length);
            return nHeader;
        } else {
            AbDataDef nDef = def.copy(skipDummyCol);
            return genHeader(nDef, data);
        }
    }

    public AbRecord fromGenericRecord(GenericRecord record, Boolean reuse) throws IOException, GdrException {
        if (reuse)
            this.data = record;
        else
            this.data = cloneData(record, Boolean.FALSE);
        return this;
    }

    protected byte[] genHeader() {
        return genHeader(def, data);
    }

    protected byte[] genHeader(AbDataDef def, GenericRecord data) {
        byte[] header0 = new byte[def.getHeaderLen()];
        int i = 0;
        for (AbColumnDef col : def.getColumnList()) {
            if (col.isNullable() && col.getNVL() == null) {
                if (data.get(col.getName()) == null) {
                    AbUtils.setBit(header0, i, Boolean.TRUE);
                } else {
                    AbUtils.setBit(header0, i, Boolean.FALSE);
                }
                i++;
            }
        }

        for (; i < header.length * 8; i++)
            AbUtils.setBit(header0, i, Boolean.FALSE);
        return header0;
    }

    public GenericRecord getData() {
        return data;
    }

    public void setData(GenericRecord data) {
        this.data = data;
    }

    public AbDataDef getDef() {
        return def;
    }

    public void setDef(AbDataDef def) {
        this.def = def;
    }

    public byte[] getHeader() {
        return header;
    }

    public void setHeader(byte[] header) {
        this.header = header;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(long bytesRead) {
        this.bytesRead = bytesRead;
    }

    public Object getValue(String cName) {
        return data.get(cName);
    }

    private boolean isNull(String name) {
        if (def.indexOfHideNullColumn(name) != null)
            return AbUtils.bitAt(header, def.indexOfHideNullColumn(name)) == 1;
        else
            return Boolean.FALSE;
    }

    public boolean read(InputStream dis) throws IOException, GdrException {
        initBytesRead();
        if (dis.available() < 1) {
            return Boolean.FALSE;
        }

        try {
            if (def.hasHideNull()) {
                int byteCnt = dis.read(header);
                if (byteCnt > 0 && byteCnt != header.length)
                    throw new EOFException("Unexpected EOF while read record header, byte read:" + byteCnt
                            + ", expect:" + header.length);
                else if (byteCnt < 0)
                    return Boolean.FALSE;
            }
        } catch (EOFException e) {
            LOG.debug("Unexpected EOF while read record header.");
            dis.close();
            throw new GdrRuntimeException(e.getLocalizedMessage());
        }

        int columnInd = 0;
        for (AbColumnDef col : def.getColumnList()) {
            Object returnValue = null;
            try {
                returnValue = col.read(dis, def.hasHideNull() ? isNull(col.getName()) : Boolean.FALSE);
                setBytesRead(getBytesRead() + col.getBytesRead());
            } catch (EOFException | GdrException | GdrRuntimeException e) {
                if (columnInd == 0
                        && !getDef().hasHideNull()
                        && e.getLocalizedMessage().startsWith("unexpected EOF for column")) {
                    LOG.debug("EOF touched.");
                    return Boolean.FALSE;
                } else
                    LOG.warn("failed to read column: " + col.getName());

                //TODO use configure file to override the setting
                if (!Constant.IGNORE_BROKEN_RECORD)
                    throw e;
                else
                    return Boolean.FALSE;
            }

            data.put(col.getName(), returnValue);
            columnInd++;
        }

        return Boolean.TRUE;
    }

    private void initBytesRead() {
        this.bytesRead = 0l;
    }

    public GenericRecord toGenericRecord(Boolean ignoreDummyCol, Boolean reuse) throws AbTypeException {
        if (!ignoreDummyCol && reuse)
            return data;
        else
            return cloneData(ignoreDummyCol);
    }

    @Override
    public String toString() {
        return toString(Boolean.FALSE);
    }

    public String toString(Boolean ignoreDummyColumn) {
        StringBuilder stringBuilder = new StringBuilder();

        for (AbColumnDef column : def.getColumnList()) {
            if (!ignoreDummyColumn || column.enableOutput()) {
                if (getValue(column.getName()) != null)
                    stringBuilder.append(column.format(getValue(column.getName())));
                stringBuilder.append(def.getOutputDel());
            }
        }

        if (stringBuilder.length() > def.getOutputDel().length())
            return stringBuilder.substring(0, stringBuilder.length() - def.getOutputDel().length());
        else
            throw new GdrRuntimeException("bad record string created: " + stringBuilder.toString());
    }

    public void write(OutputStream dos) throws IOException, GdrException {
        write(dos, Boolean.FALSE, Boolean.TRUE);
    }

    public void write(OutputStream dos, Boolean defaultToNull, Boolean overrideHead) throws GdrException, IOException {
        if (data == null) {
            return;
        }
        if (def.hasHideNull())
            writeHeader(dos, overrideHead);
        writeRow(dos, defaultToNull);
    }

    protected void writeHeader(OutputStream dos, boolean override) throws IOException {
        dos.write(header == null || override ? genHeader() : header);
        dos.flush();
    }

    protected void writeRow(OutputStream dos, Boolean defaultToNull) throws IOException, GdrException {
        for (int i = 0; i < def.getColumnList().size(); i++) {
            def.getColumn(i).write(dos, data.get(i), defaultToNull);
            dos.flush();
        }
    }
}
