package org.apache.gdr.mapred;

import org.apache.gdr.common.AbRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class AbAsTextRecordReader extends AbRecordReader<LongWritable, Text> {
    static final Log LOG = LogFactory.getLog(AbAsTextRecordReader.class);

    public AbAsTextRecordReader(JobConf conf, FileSplit split) throws IOException {
        super(conf, split);
    }

    @Override
    public boolean next(LongWritable key, Text value) throws IOException {
        if (getFilePosition() <= stop) {
            try {
                key.set(getPos());
                AbRecord ab = reader.readRecord(abFileIn);
                if (ab == null)
                    return false;
                pos += ab.getBytesRead();
                String rtn = ab.toString(ignoreDummyColumn);
                if (rtn != null) {
                    value.set(rtn);
                    return true;
                }
            } catch (Exception e) {
                LOG.error("ERROR on offset:" + this.getPos());
                e.printStackTrace();
                if (e instanceof RuntimeException) {

                }
                throw new RuntimeException("record read error.");
            }
        }
        return false;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }
}
