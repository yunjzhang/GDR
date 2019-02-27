package org.apache.gdr.ext.tool.mapreduce;

import org.apache.gdr.mapreduce.AbAsTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class AbAsTextConverterMapReduce extends Configured implements Tool {

    public static void main(String[] args) {
        int exitCode = 1;
        try {
            exitCode = ToolRunner.run(new AbAsTextConverterMapReduce(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.exit(exitCode);
        }
    }

    public int run(String[] args) throws Exception {
        String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

        if (otherArgs.length != 3 && args.length != 4) {
            printUsage();
            System.exit(1);
        }
        String dmlPath = otherArgs[2];

        Job job = new Job(getConf(), "Ab convert job");
        job.getConfiguration().set(AbAsTextInputFormat.AB_DML_FILE, dmlPath);
        System.out.println(job.getConfiguration().get(AbAsTextInputFormat.AB_DML_FILE));

        try {

            job.setJarByClass(this.getClass());

            // reducer
            job.setReducerClass(NullKeyIdentityReducer.class);

            // map output key/value
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);

            // reduce output key/value
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            // set the converter on the input format
            job.setInputFormatClass(AbAsTextInputFormat.class);

            // set the output format
            job.setOutputFormatClass(TextOutputFormat.class);

            // compress at the block level
            Configuration conf = job.getConfiguration();
            conf.setBoolean("mapred.output.compress", false);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            if (args.length == 4) {
                conf.set("mapreduce.job.queuename", args[3]);
            }

            return job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            return 2;
        }
    }

    private void printUsage() {
        System.out.println("Usage: AbToTextConverterMapReduce <in_dir> <out_dir> <dmlfile> [queue]");
    }

    private static class NullKeyIdentityReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
        private NullWritable NULL_KEY = NullWritable.get();

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext())
                context.write(NULL_KEY, iterator.next());
        }
    }


}
