package org.apache.gdr.mapreduce;

import org.apache.gdr.common.schema.dataset.AbDataDef;
import org.apache.gdr.common.util.DmlParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedInputStream;
import java.io.IOException;

public abstract class AbRecordReader<K, V> extends RecordReader<K, V> {
    protected AbDataDef abRecordReader;
    protected long m_recordCount = 0l;
    protected K key;
    protected V value;
    protected FSDataInputStream m_dis;
    protected boolean done = false;
    private CompressionCodecFactory compressionCodecs = null;
    private FSDataInputStream fileIn;
    private long start;
    private long stop;

    public AbRecordReader(String dmlStr) {
        this(AbDataDef.DUMMYNAME, dmlStr);
    }

    public AbRecordReader(String name, String dmlStr) {
        try {
            abRecordReader = DmlParser.parseDml(name, dmlStr);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("unable to parse dml.");
        }
    }

    @Override
    public void close() throws IOException {
        m_dis.close();
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public long getPos() throws IOException {
        return fileIn.getPos();
    }

    @Override
    public float getProgress() throws IOException {
        return stop == start ? 0.0f : Math.min(1.0f, (getPos() - start) / (float) (stop - start));
    }

    @Override
    public void initialize(InputSplit inputsplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputsplit;
        Configuration job = context.getConfiguration();

        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(split.getPath());
        if (codec != null) {
            m_dis = new FSDataInputStream(new BufferedInputStream(codec.createInputStream(fileIn), 65536));
        } else {
            m_dis = fileIn;
        }

        this.start = fileIn.getPos();
        this.stop = split.getStart() + split.getLength();

        if (start >= stop)
            done = true;

        key = initKey();

        value = initValue();
    }

    public abstract K initKey();

    public abstract V initValue();
}
