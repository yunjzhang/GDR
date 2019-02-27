package org.apache.gdr.mapred;

import org.apache.gdr.hive.serde.abvro.AbvroSerDe;
import org.apache.gdr.mapred.conf.Constant;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.junit.Test;

import java.util.Properties;

public class testSerDe {
    //@Test
    public void testAbvroSerDe() throws Exception {
        String dml = "*.dml";
        String dat = "*.dat";
        JobConf conf = new JobConf();
        conf.set(Constant.AB_DML_FILE, dml);

        AbvroSerDe serDe = new AbvroSerDe();
        Properties properties = new Properties();
        properties.put(Constant.AB_DML_FILE, dml);
        serDe.initialize(conf, properties);

        AbAsAvroInputFormat inputFormat = new AbAsAvroInputFormat();
        inputFormat.configure(conf);

        // Open the file using TextInputFormat
        AbAsAvroInputFormat.addInputPaths(conf, dat);
        InputSplit[] splits = inputFormat.getSplits(conf, 1);
        RecordReader<NullWritable, AvroGenericRecordWritable> recordReader =
                inputFormat.getRecordReader(splits[0], conf, Reporter.NULL);

        // Verify the data
        NullWritable key = recordReader.createKey();
        AvroGenericRecordWritable value = recordReader.createValue();
        long l = 0l;
        long s = System.nanoTime();
        Log.info("start: " + s);
        while (recordReader.next(key, value)) {
            Object o = serDe.deserialize(value);
            l++;
            //System.out.println(key);
        }
        long e = System.nanoTime();
        Log.info("end: " + e);
        Log.info("duration millisec: " + (e - s) / 1000000);
        Log.info("record read: " + l);
        Log.info("rec/sec: " + l * 1000000000 / (e - s));
    }
}
