package org.apache.gdr.mapred;

import org.apache.gdr.mapred.conf.Constant;
import junit.framework.Assert;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;

public class TestInputFormat {
    static org.apache.commons.logging.Log LOG = LogFactory.getLog(TestInputFormat.class);

    static Map<String, String> inputMap = new HashMap<>();
    static Map<String, String> gzInputMap = new HashMap<>();

    private static File testDir;

    static {
        testDir = new File("target/dat");


        if (!testDir.exists())
            testDir.mkdir();

        for (Entry<String, String> kv : inputMap.entrySet()) {
            String gz = testDir.getPath() + "/"
                    + kv.getValue().substring(kv.getValue().lastIndexOf("/")) + ".gz";
            gzInputMap.put(kv.getKey(), gz);
            createGzFile(kv.getValue(), gz);
        }
    }

    static void createGzFile(String source_filepath, String destinaton_zip_filepath) {

        byte[] buffer = new byte[1024];

        try {

            FileOutputStream fileOutputStream = new FileOutputStream(destinaton_zip_filepath);

            GZIPOutputStream gzipOuputStream = new GZIPOutputStream(fileOutputStream);

            FileInputStream fileInput = new FileInputStream(source_filepath);

            int bytes_read;

            while ((bytes_read = fileInput.read(buffer)) > 0) {
                gzipOuputStream.write(buffer, 0, bytes_read);
            }

            fileInput.close();

            gzipOuputStream.finish();
            gzipOuputStream.close();

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void ComparePlain2GzInput() throws Exception {
        for (Entry<String, String> kv : inputMap.entrySet()) {
            String dml = kv.getKey();
            String dat = kv.getValue();
            LOG.info(" working reread on file: " + dat);

            JobConf conf = new JobConf();
            conf.set(Constant.AB_DML_FILE, dml);
            conf.set("mapreduce.task.attempt.id", "attempt_123_456_m_789_1");

            //input
            conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, dat);
            AbAsAvroInputFormat textInputFormat = new AbAsAvroInputFormat();
            textInputFormat.configure(conf);
            InputSplit[] splits = textInputFormat.getSplits(conf, 1);
            RecordReader<NullWritable, AvroGenericRecordWritable> recordReader =
                    textInputFormat.getRecordReader(splits[0], conf, Reporter.NULL);

            // Verify the data
            NullWritable key = recordReader.createKey();
            AvroGenericRecordWritable value = recordReader.createValue();
            long plainCnt = 0l;
            while (recordReader.next(key, value)) {
                plainCnt++;
            }

            //start gz read
            String gz = gzInputMap.get(dml);
            //input
            conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, gz);
            textInputFormat = new AbAsAvroInputFormat();
            textInputFormat.configure(conf);
            splits = textInputFormat.getSplits(conf, 1);
            recordReader = textInputFormat.getRecordReader(splits[0], conf, Reporter.NULL);

            // Verify the data
            long gzCnt = 0l;
            while (recordReader.next(key, value)) {
                gzCnt++;
            }

            Assert.assertEquals("Row count from plain file read different to gz file", plainCnt, gzCnt);
        }
    }

    @Test
    public void testAbInputFormat() throws Exception {
        for (Entry<String, String> kv : inputMap.entrySet()) {
            String dml = kv.getKey();
            String dat = kv.getValue();
            LOG.info(" working reread on file: " + dat);

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

            // Verify the data
            NullWritable key = recordReader.createKey();
            AvroGenericRecordWritable value = recordReader.createValue();
            long l = 0l;
            long s = System.nanoTime();
            //Log.info("start: " + s);
            while (true) {
                if (!recordReader.next(key, value)) break;
                l++;
            }

            long e = System.nanoTime();
            //Log.info("end: " + e);
            //Log.info("duration millisec: " + (e-s));
            Log.info("record read: " + l);
            //Log.info("rec/sec: " + l*1000/(e-s));
            //Log.info("last rec: " + value.getRecord().toString());
        }
    }

    @Test
    public void testAbGzInputFormat() throws Exception {
        for (Entry<String, String> kv : gzInputMap.entrySet()) {
            String dml = kv.getKey();
            String dat = kv.getValue();
            LOG.info(" working reread on file: " + dat);

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

            // Verify the data
            NullWritable key = recordReader.createKey();
            AvroGenericRecordWritable value = recordReader.createValue();
            long l = 0l;
            long s = System.nanoTime();
            //Log.info("start: " + s);
            while (true) {
                if (!recordReader.next(key, value)) break;
                l++;
            }

            long e = System.nanoTime();
            //Log.info("end: " + e);
            //Log.info("duration millisec: " + (e-s)/1000000);
            Log.info("record read: " + l);
            //Log.info("rec/sec: " + l*1000000000/(e-s));
            //Log.info("last rec: " + value.getRecord().toString());
        }
    }
}
