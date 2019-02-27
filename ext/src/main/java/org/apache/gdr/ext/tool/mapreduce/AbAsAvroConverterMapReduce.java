package org.apache.gdr.ext.tool.mapreduce;

import org.apache.gdr.common.util.DmlParser;
import org.apache.gdr.mapreduce.AbASAvroInputFormat;
import org.apache.gdr.mapreduce.AbAsTextInputFormat;
import org.apache.gdr.mapreduce.OptionsHelper;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;

public class AbAsAvroConverterMapReduce extends Configured implements Tool {
    public static final Option INPUT = newOption("path", true, true, "input path", "input");
    public static final Option OUTPUT = newOption("path", true, true, "output path", "output");
    public static final Option DML = newOption("path", true, true, "dml path", "dml");
    protected Options options = new Options();
    protected OptionsHelper optionsHelper = new OptionsHelper();

    public static void main(String[] args) {
        int exitCode = 1;
        try {
            exitCode = ToolRunner.run(new AbAsAvroConverterMapReduce(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.exit(exitCode);
        }
    }

    @SuppressWarnings("static-access")
    public static Option newOption(String argName, boolean hasArg, boolean required, String desc, String opt) {
        return OptionBuilder.withArgName(argName).hasArg(hasArg).isRequired(required).withDescription(desc).create(opt);
    }

    Schema getAvroSchema(Job job, String filePath) throws Exception {
        Path path = new Path(filePath);
        FileSystem fs = FileSystem.get(path.toUri(), job.getConfiguration());
        FSDataInputStream dmlfile = fs.open(path);
        StringBuilder sb = new StringBuilder();

        byte[] b = new byte[100];
        int rtn = 0;
        while (dmlfile.available() > 0) {
            rtn = dmlfile.read(b);
            sb.append(new String(b).substring(0, rtn));
        }
        return DmlParser.parseDml(sb.toString()).getAvroSchema();
    }

    protected String getOptionValue(Option option) {
        return optionsHelper.getOptionValue(option);
    }

    public void parseArgs(String[] args) throws ParseException {
        options.addOption(INPUT);
        options.addOption(OUTPUT);
        options.addOption(DML);
        optionsHelper.parseOptions(this.options, args);
    }

    public int run(String[] args) throws Exception {
        parseArgs(args);

        String dmlPath = optionsHelper.getOptionValue(DML);

        Job job = new Job(getConf(), "Ab convert job");

        Configuration conf = job.getConfiguration();
        conf.set(AbAsTextInputFormat.AB_DML_FILE, dmlPath);
        conf.setBoolean("mapred.output.compress", false);
        conf.set("mapreduce.job.user.classpath.first", "true");

        try {
            job.setJarByClass(this.getClass());

            // map output key/value
            job.setMapOutputKeyClass(AvroKey.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setNumReduceTasks(0);

            // set the converter on the input format
            job.setInputFormatClass(AbASAvroInputFormat.class);
            job.setOutputFormatClass(AvroKeyOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path(optionsHelper.getOptionValue(INPUT)));
            FileOutputFormat.setOutputPath(job, new Path(optionsHelper.getOptionValue(OUTPUT)));

            Schema schema = getAvroSchema(job, dmlPath);
            AvroJob.setOutputKeySchema(job, schema);

            Log.info(schema.toString());

            return job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            return 2;
        }
    }
}
