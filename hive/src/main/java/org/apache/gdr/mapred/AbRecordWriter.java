package org.apache.gdr.mapred;

import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.schema.dataset.AbDataDef;
import org.apache.gdr.common.util.DmlParser;
import org.apache.gdr.mapred.conf.Constant;
import org.apache.gdr.mapred.util.ContextTool;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.OutputStream;

public abstract class AbRecordWriter<K, V> implements RecordWriter<K, V> {
    static Log LOG = LogFactory.getLog(AbRecordWriter.class);
    protected OutputStream fileOut;
    protected AbDataDef recDef;

    public AbRecordWriter(JobConf conf, String name, Progressable progress) throws IOException {
        boolean isCompressed = FileOutputFormat.getCompressOutput(conf) || conf.getBoolean(Constant.GDR_COMPRESS_OUTPUT, false);

        if (!isCompressed) {
            Path file = FileOutputFormat.getTaskOutputPath(conf, name);
            FileSystem fs = file.getFileSystem(conf);
            fileOut = fs.create(file, progress);
        } else {
            Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(conf,
                    GzipCodec.class);

            String codecStr = conf.get(Constant.GDR_COMPRESS_CODEC);
            if (!StringUtils.isBlank(codecStr)) {
                try {
                    if (Class.forName(codecStr.trim()).isInstance(CompressionCodec.class))
                        codecClass = Class.forName(codecStr.trim()).asSubclass(CompressionCodec.class);
                    else
                        throw new GdrRuntimeException("unsupported compress codec: " + codecStr);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    throw new GdrRuntimeException(e.getLocalizedMessage());
                }
            }

            // create the named codec
            CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
            // build the filename including the extension
            Path file = FileOutputFormat.getTaskOutputPath(conf, name + codec.getDefaultExtension());
            FileSystem fs = file.getFileSystem(conf);
            fileOut = codec.createOutputStream(fs.create(file, progress));
        }

        try {
            String dmlFile = conf.get(Constant.AB_DML_FILE);
            if (StringUtils.isNotBlank(dmlFile))
                recDef = DmlParser.parseDml(ContextTool.getDmlName(dmlFile), ContextTool.getDmlDefine(conf));
            else {
                recDef = DmlParser.parseDml(AbDataDef.DUMMYNAME, conf.get(Constant.AB_DML_STRING));
            }
            LOG.debug(recDef.getAvroSchema());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("unable to parse dml.");
        }
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        fileOut.close();
    }
}
