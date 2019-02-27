package org.apache.gdr.ext.tool.mapred;

import org.apache.gdr.mapred.AbAsAvroInputFormat;
import org.apache.gdr.mapred.AvroAsAbOutputFormat;
import org.apache.gdr.mapred.conf.Constant;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;


public class Ab2Avro2AbConverterMapReduce {
    public static void main(String[] args) throws Exception {
        if (args.length != 3 && args.length != 4) {
            printUsage();
            System.exit(1);
        }
        String dmlPath = args[2];

        JobConf conf = new JobConf();
        conf.setJarByClass(Ab2Avro2AbConverterMapReduce.class);

        conf.set(Constant.AB_DML_FILE, dmlPath);

        // map output key/value
        conf.setMapOutputKeyClass(NullWritable.class);
        conf.setMapOutputValueClass(AvroGenericRecordWritable.class);

        // set the converter on the input format
        conf.setInputFormat(AbAsAvroInputFormat.class);

        // set the output format
        conf.setOutputFormat(AvroAsAbOutputFormat.class);

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
        System.out.println("Usage: Ab2Avro2AbConverterMapReduce <in_dir> <out_dir> <dmlfile> [queue]");
    }
}
