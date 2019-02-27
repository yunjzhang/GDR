package org.apache.gdr.mapred;

import org.apache.gdr.common.schema.dataset.AbDataDef;
import org.apache.gdr.common.util.DmlParser;
import org.apache.gdr.mapred.conf.Constant;
import org.apache.gdr.mapred.util.ContextTool;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public abstract class AbRecordReader<K, V> implements RecordReader<K, V>, JobConfigurable {
    static Log LOG = LogFactory.getLog(AbRecordReader.class);

    protected AbDataDef reader;
    protected Boolean ignoreDummyColumn;
    protected long start;
    protected long pos;
    protected long stop;
    protected long rowCount = 0;
    protected JobConf jobConf;
    protected long m_recordCount = 0l;
    protected FSDataInputStream rawFileIn;
    protected DataInputStream abFileIn = null;
    CompressionCodec codec = null;

    public AbRecordReader(JobConf conf, FileSplit split) throws IOException {
        start = split.getStart();
        stop = start + split.getLength();
        pos = start;
        final Path file = split.getPath();
        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
        codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(conf);
        rawFileIn = fs.open(file);
        LOG.debug("bytes to read from " + file.toUri() + ":" + stop);
        if (codec != null) {
            ;

            Decompressor decompressor = CodecPool.getDecompressor(codec);
            Integer oldBuff = conf.getInt(DFSConfigKeys.IO_FILE_BUFFER_SIZE_KEY, -1);
            conf.setInt(DFSConfigKeys.IO_FILE_BUFFER_SIZE_KEY, conf.getInt(Constant.GDR_FILE_BUFFER_SIZE, 64*1024));
            abFileIn = new DataInputStream(new BufferedInputStream(codec.createInputStream(rawFileIn, decompressor),
                    conf.getInt(Constant.GDR_FILE_BUFFER_SIZE, 64*1024)));
            if (oldBuff > 0)
                conf.setInt(DFSConfigKeys.IO_FILE_BUFFER_SIZE_KEY, oldBuff);
        } else {
            rawFileIn.seek(start);
            abFileIn = new DataInputStream(new BufferedInputStream(rawFileIn, 65536));
        }

        String dmlFile = null;
        try {
            ignoreDummyColumn = ContextTool.skipDummyColumn(conf);
            dmlFile = conf.get(Constant.AB_DML_FILE);
            if (StringUtils.isNotBlank(dmlFile))
                reader = DmlParser.parseDml(ContextTool.getDmlName(dmlFile), ContextTool.getDmlDefine(conf));
            else {
                reader = DmlParser.parseDml(AbDataDef.DUMMYNAME, conf.get(Constant.AB_DML_STRING));
            }
            LOG.debug(reader.getAvroSchema());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("unable to parse dml:" + dmlFile);
        }
    }

    @Override
    public long getPos() throws IOException {
        return rawFileIn.getPos();
    }

    @Override
    public void close() throws IOException {
        rawFileIn.close();
    }

    @Override
    public float getProgress() throws IOException {
        return stop == start ? 0.0f : Math.min(1.0f, (getPos() - start) / (float) (stop - start));
    }

    @Override
    public void configure(JobConf jobConf) {
        this.jobConf = jobConf;
    }

    protected long getFilePosition() throws IOException {
        long retVal;
        if (isCompressedInput() && null != rawFileIn) {
            retVal = getPos();
        } else {
            retVal = pos;
        }
        return retVal;
    }

    private boolean isCompressedInput() {
        return (codec != null);
    }
}
