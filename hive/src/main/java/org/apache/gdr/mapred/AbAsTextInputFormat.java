package org.apache.gdr.mapred;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class AbAsTextInputFormat extends AbInputFormat<LongWritable, Text> {
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf jc, Reporter reporter)
            throws IOException {
        reporter.setStatus(inputSplit.toString());
        return new AbAsTextRecordReader(jc, (FileSplit) inputSplit);
    }
}
