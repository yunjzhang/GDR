package org.apache.gdr.mapred.util;

import org.apache.gdr.common.schema.dataset.AbDataDef;
import org.apache.gdr.mapred.conf.Constant;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class ContextTool {
    public static String getDmlDefine(JobConf conf) throws IOException {
        String filePath = conf.get(Constant.AB_DML_FILE);
        Path file = new Path(filePath);
        FileSystem fs = file.getFileSystem(conf);
        if (StringUtils.isNotBlank(filePath)) {
            return readDmlFile(file, fs);
        }
        return null;
    }

    public static String readDmlFile(Path filePath, FileSystem fs) throws IOException {
        FSDataInputStream dmlfile = fs.open(filePath);
        StringBuilder sb = new StringBuilder();

        byte[] b = new byte[1024];
        int rtn = 0;
        while (dmlfile.available() > 0) {
            rtn = dmlfile.read(b);
            sb.append(new String(b).substring(0, rtn));
        }
        return sb.toString();

    }

    public static String getDmlName(String dmlFile) {
        String dmlName = null;
        Path dmlPath = new Path(dmlFile);
        String[] strs = dmlPath.getName().replace(".dml", "").split("\\.", 3);

        if (strs.length > 1) {
            dmlName = strs[1];
        } else {
            dmlName = strs[0];
        }

        if (StringUtils.isBlank(dmlName))
            dmlName = AbDataDef.DUMMYNAME;

        return dmlName;
    }

    public static Boolean skipDummyColumn(JobConf conf) {
        return conf.getBoolean(Constant.AB_DML_IGNORE_DUMMY_COLUMN, false);
    }
}
