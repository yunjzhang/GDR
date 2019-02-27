package org.apache.gdr.ext.tool.mapred;

import org.apache.gdr.mapred.AbAsTextInputFormat;
import org.apache.gdr.mapred.conf.Constant;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


public class AbAsTextConverterMapReduce {
    public static void main(String[] args) throws Exception {
        if (args.length != 3 && args.length != 4) {
            printUsage();
            System.exit(1);
        }
        String dmlPath = args[2];

        JobConf conf = new JobConf();
        conf.setJarByClass(AbAsTextConverterMapReduce.class);

        conf.set(Constant.AB_DML_FILE, dmlPath);

        // map output key/value
        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(Text.class);

        // set the converter on the input format
        conf.setInputFormat(AbAsTextInputFormat.class);

        // set the output format
        conf.setOutputFormat(TextOutputFormat.class);

        // compress at the block level
        conf.setBoolean("mapred.output.compress", false);

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        if (args.length == 4) {
            conf.set("mapred.job.queue.name", args[3]);
        }

        RunningJob job = JobClient.runJob(conf);
        if (job.getJobState() == 1) {
            System.out.println("Job is successfully done.");
        }
    }

    private static void printUsage() {
        System.out.println("Usage: AbToTextConverterMapReduce <in_dir> <out_dir> <dmlfile> [queue]");
    }
}
