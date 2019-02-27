package org.apache.gdr.hive.ql.io.ab;

import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.mapred.conf.Constant;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class AvroAsAbOutputFormat extends AbOutputFormat<NullWritable, AvroGenericRecordWritable> {
    @Override
    public RecordWriter<NullWritable, AvroGenericRecordWritable> getRecordWriter(FileSystem ignored, JobConf job,
                                                                                 String name, Progressable progress) throws IOException {
        return new org.apache.gdr.mapred.AvroAsAbRecordWriter(job, name, progress);
    }

    @Override
    public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc,
                                                                                            Path path, Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties,
                                                                                            Progressable progress) throws IOException {
        OutputStream os = null;
        if (isCompressed || jc.getBoolean(Constant.GDR_COMPRESS_OUTPUT, false)) {
            Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(jc, GzipCodec.class);
            String codecStr = jc.get(Constant.GDR_COMPRESS_CODEC);
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


            CompressionCodec codec = ReflectionUtils.newInstance(codecClass, jc);
            path = new Path(path.toString() + codec.getDefaultExtension());
            os = codec.createOutputStream(path.getFileSystem(jc).create(path, progress));
        } else {
            os = path.getFileSystem(jc).create(path, progress);
        }
        return new AvroAsAbRecordWriter(jc, os);
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        return;
    }
}
