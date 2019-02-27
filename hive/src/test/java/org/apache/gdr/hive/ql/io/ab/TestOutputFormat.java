package org.apache.gdr.hive.ql.io.ab;

import org.apache.gdr.mapred.AbAsAvroInputFormat;
import org.apache.gdr.mapred.conf.Constant;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class TestOutputFormat {
    static org.apache.commons.logging.Log LOG = LogFactory.getLog(TestOutputFormat.class);

    static Map<String, String> inputMap = new HashMap<>();
    static File testDir = null;

    static {
        testDir = new File("target/dat");

        if (!testDir.exists())
            testDir.mkdir();
    }

    @Test
    public void testOutputFormat() throws Exception {
        for (Entry<String, String> kv : inputMap.entrySet()) {
            String dml = kv.getKey();
            String dat = kv.getValue();
            LOG.info(" working reread on file: " + dat);

            String out = testDir.getPath() + "/"
                    + kv.getValue().substring(kv.getValue().lastIndexOf("/"));
            File f = new File(out).getParentFile();
            if (!f.exists()) {
                f.mkdir();
            }

            JobConf conf = new JobConf();
            conf.set(Constant.AB_DML_FILE, dml);
            conf.set("mapreduce.task.attempt.id", "attempt_123_456_m_789_1");

            //input
            AbAsAvroInputFormat textInputFormat = new AbAsAvroInputFormat();
            textInputFormat.configure(conf);
            AbAsAvroInputFormat.addInputPaths(conf, dat);
            InputSplit[] splits = textInputFormat.getSplits(conf, 1);
            RecordReader<NullWritable, AvroGenericRecordWritable> recordReader =
                    textInputFormat.getRecordReader(splits[0], conf, Reporter.NULL);

            //output
            AvroAsAbOutputFormat outFormat = new AvroAsAbOutputFormat();
            outFormat.configure(conf);
            org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter writer =
                    outFormat.getHiveRecordWriter(conf, new Path(out), null, false, null, null);

            // Verify the data
            NullWritable key = recordReader.createKey();
            AvroGenericRecordWritable value = recordReader.createValue();
            long l = 0l;
            long s = System.nanoTime();
            Log.info("start: " + s);
            while (true) {
                if (!recordReader.next(key, value)) break;
                writer.write(value);
                l++;
            }
            writer.close(false);
            long e = System.nanoTime();
            Log.info("end: " + e);
            Log.info("duration millsec: " + (e - s) / 1000000);
            Log.info("record read: " + l);
            Log.info("rec/sec: " + l * 1000000000 / (e - s));
            //Log.info("last rec: " + value.getRecord().toString());
        }
    }

    @Test
    public void testGzOutputFormat() throws Exception {
        for (Entry<String, String> kv : inputMap.entrySet()) {
            String dml = kv.getKey();
            String dat = kv.getValue();
            LOG.info(" working reread on file: " + dat);

            String out = testDir.getPath() + "/"
                    + kv.getValue().substring(kv.getValue().lastIndexOf("/"));
            File f = new File(out).getParentFile();
            if (!f.exists()) {
                f.mkdir();
            }

            JobConf conf = new JobConf();
            conf.set(Constant.AB_DML_FILE, dml);
            conf.set("mapreduce.task.attempt.id", "attempt_123_456_m_789_1");
            conf.setBoolean(FileOutputFormat.COMPRESS, false);
            conf.set(FileOutputFormat.COMPRESS_CODEC, GzipCodec.class.getName());

            //input
            AbAsAvroInputFormat textInputFormat = new AbAsAvroInputFormat();
            textInputFormat.configure(conf);
            AbAsAvroInputFormat.addInputPaths(conf, dat);
            InputSplit[] splits = textInputFormat.getSplits(conf, 1);
            RecordReader<NullWritable, AvroGenericRecordWritable> recordReader =
                    textInputFormat.getRecordReader(splits[0], conf, Reporter.NULL);

            //output
            AvroAsAbOutputFormat outFormat = new AvroAsAbOutputFormat();
            outFormat.configure(conf);
            org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter writer =
                    outFormat.getHiveRecordWriter(conf, new Path(out), null, true, null, null);

            // Verify the data
            NullWritable key = recordReader.createKey();
            AvroGenericRecordWritable value = recordReader.createValue();
            long l = 0l;
            long s = System.nanoTime();
            Log.info("start: " + s);
            while (true) {
                if (!recordReader.next(key, value)) break;
                writer.write(value);
                l++;
            }
            writer.close(false);
            long e = System.nanoTime();
            Log.info("end: " + e);
            Log.info("duration millsec: " + (e - s) / 1000000);
            Log.info("record read: " + l);
            Log.info("rec/sec: " + l * 1000000000 / (e - s));
            //Log.info("last rec: " + value.getRecord().toString());
        }
    }
}
