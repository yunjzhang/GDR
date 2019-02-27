package org.apache.gdr.mapreduce;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class AbASAvroInputFormat extends AbInputFormat<AvroKey<GenericRecord>, NullWritable> {
    @Override
    public RecordReader<AvroKey<GenericRecord>, NullWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        context.setStatus(split.toString());

        return new AbAsAvroRecordReader(getDmlName(context), getDmlDefine(context));
    }
}
