package org.apache.gdr.hive.ql.io.ab;

import org.apache.gdr.common.AbRecord;
import org.apache.gdr.common.exception.GdrException;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.OutputStream;

public class AvroAsAbRecordWriter extends AbRecordWriter {

    public AvroAsAbRecordWriter(JobConf conf, OutputStream fileOut) {
        super(conf, fileOut);
    }

    @Override
    public void write(Writable writable) throws IOException {
        if (!(writable instanceof AvroGenericRecordWritable)) {
            throw new IOException("Expecting instance of AvroGenericRecordWritable, " + "but received"
                    + writable.getClass().getCanonicalName());
        }
        AvroGenericRecordWritable r = (AvroGenericRecordWritable) writable;
        try {
            AbRecord record = new AbRecord(recDef);
            record.fromGenericRecord(r.getRecord(), true);
            record.write(fileOut, true, true);
        } catch (GdrException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getLocalizedMessage());
        }
    }
}
