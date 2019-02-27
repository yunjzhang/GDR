package org.apache.gdr.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class AbAsJsonInputFormat extends AbInputFormat<LongWritable, Text> {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        context.setStatus(split.toString());

        return new AbAsJsonRecordReader(getDmlName(context), getDmlDefine(context));
    }
}
