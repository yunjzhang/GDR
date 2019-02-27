package org.apache.gdr.mapreduce;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class AbAsAvroRecordReader extends AbRecordReader<AvroKey<GenericRecord>, NullWritable> {
    public AbAsAvroRecordReader(String name, String dmlStr) {
        super(name, dmlStr);
    }

    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        GenericRecord rec;
        try {
            rec = abRecordReader.readRecord(m_dis).getData();
            if (rec != null) {
                key.datum(rec);
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("record read error.");
        }
        return false;
    }

    @Override
    public NullWritable initValue() {
        return NullWritable.get();
    }

    @Override
    public AvroKey<GenericRecord> initKey() {
        return new AvroKey<>();
    }
}
