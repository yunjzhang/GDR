package org.apache.gdr.mapred;

import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class AbAsAvroInputFormat extends AbInputFormat<NullWritable, AvroGenericRecordWritable> {
    public RecordReader<NullWritable, AvroGenericRecordWritable> getRecordReader(InputSplit inputSplit, JobConf jc,
                                                                                 Reporter reporter) throws IOException {
        reporter.setStatus(inputSplit.toString());
        return new AbAsAvroRecordReader(jc, (FileSplit) inputSplit);
    }
}
