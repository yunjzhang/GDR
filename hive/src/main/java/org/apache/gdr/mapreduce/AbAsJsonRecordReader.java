package org.apache.gdr.mapreduce;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class AbAsJsonRecordReader extends AbRecordReader<LongWritable, Text> {

    private int m_recordCount = 0;

    public AbAsJsonRecordReader(String name, String dmlStr) {
        super(name, dmlStr);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        String convertedValue;
        GenericRecord rec;
        try {
            rec = abRecordReader.readRecord(m_dis).getData();
            if (rec != null) {
                convertedValue = rec.toString();
                key.set(++m_recordCount);
                value.set(convertedValue);
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("record read error.");
        }
        return false;
    }

    @Override
    public LongWritable initKey() {
        return new LongWritable();
    }

    @Override
    public Text initValue() {
        return new Text();
    }
}
