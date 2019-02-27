package org.apache.gdr.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class AbAsTextRecordReader extends AbRecordReader<LongWritable, Text> {
    public AbAsTextRecordReader(String name, String dmlStr) {
        super(name, dmlStr);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (done)
            return false;

        try {
            String rtn = abRecordReader.readRecord(m_dis).toString(false);
            if (rtn != null) {
                value.set(rtn);
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("record read error.");
        }
        return !(done = true);
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
