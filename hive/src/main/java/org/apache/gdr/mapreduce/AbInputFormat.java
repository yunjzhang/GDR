package org.apache.gdr.mapreduce;

import org.apache.gdr.common.schema.dataset.AbDataDef;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public abstract class AbInputFormat<K, V> extends FileInputFormat<K, V> {
    public static final String AB_DML_FILE = "com.ebay.dss.dml.file";
    public static final String AB_DML_STRING = "com.ebay.dss.dml.string";

    public static String getDmlName(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        String dmlName = null;

        String filePath = conf.get(AB_DML_FILE);
        if (StringUtils.isNotBlank(filePath)) {
            Path dmlfile = new Path(filePath);
            String[] strs = dmlfile.getName().replace(".dml", "").split("\\.", 3);
            if (strs.length > 1) {
                dmlName = strs[1];
            } else {
                dmlName = strs[0];
            }
        }

        if (StringUtils.isBlank(dmlName))
            dmlName = AbDataDef.DUMMYNAME;

        return dmlName;
    }

    public static String getDmlDefine(JobContext context) throws IllegalArgumentException, IOException {
        Configuration conf = context.getConfiguration();

        String dmlStr;
        String filePath = conf.get(AB_DML_FILE);
        Path path = new Path(filePath);
        if (StringUtils.isNotBlank(filePath)) {
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            FSDataInputStream dmlfile = fs.open(path);
            StringBuilder sb = new StringBuilder();

            byte[] b = new byte[1024];
            int rtn = 0;
            while (dmlfile.available() > 0) {
                rtn = dmlfile.read(b);
                sb.append(new String(b).substring(0, rtn));
            }
            dmlStr = sb.toString();
        } else {
            dmlStr = conf.get(AB_DML_STRING);
        }
        return dmlStr;
    }

    public static FSDataInputStream getInputStream(JobContext context) throws IllegalArgumentException, IOException {
        Configuration conf = context.getConfiguration();

        String dmlStr = conf.get(AB_DML_STRING);
        if (StringUtils.isBlank(dmlStr)) {
            Path path = new Path(conf.get(AB_DML_FILE));
            FileSystem fs = FileSystem.get(path.toUri(), context.getConfiguration());
            FSDataInputStream dmlfile = fs.open(path);
            return dmlfile;
        }
        return null;
    }

    @Override
    final protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
