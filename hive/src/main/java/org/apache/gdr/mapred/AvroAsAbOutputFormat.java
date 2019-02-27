package org.apache.gdr.mapred;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class AvroAsAbOutputFormat extends AbOutputFormat<NullWritable, AvroGenericRecordWritable> {
    @Override
    public RecordWriter<NullWritable, AvroGenericRecordWritable> getRecordWriter(FileSystem ignored, JobConf job,
                                                                                 String name, Progressable progress) throws IOException {
        return new AvroAsAbRecordWriter(job, name, progress);
    }
}
