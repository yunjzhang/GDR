package org.apache.gdr.mapred;

import org.apache.gdr.common.AbRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class AvroAsAbRecordWriter extends AbRecordWriter<NullWritable, AvroGenericRecordWritable> {
    static Log LOG = LogFactory.getLog(AvroAsAbRecordWriter.class);

    public AvroAsAbRecordWriter(JobConf conf, String name, Progressable progress) throws IOException {
        super(conf, name, progress);
    }

    @Override
    public void write(NullWritable key, AvroGenericRecordWritable value) throws IOException {
        try {
            AbRecord record = new AbRecord(recDef);
            record.fromGenericRecord(value.getRecord(), true);
            record.write(fileOut, true, true);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }
}
