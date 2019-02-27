package org.apache.gdr.hive.ql.io.ab;

import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.schema.dataset.AbDataDef;
import org.apache.gdr.common.util.DmlParser;
import org.apache.gdr.mapred.conf.Constant;
import org.apache.gdr.mapred.util.ContextTool;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.OutputStream;

public abstract class AbRecordWriter implements RecordWriter {
    static Log LOG = LogFactory.getLog(AbRecordWriter.class);
    protected OutputStream fileOut;
    protected AbDataDef recDef;
    protected Boolean ignoreDummyColumn;

    public AbRecordWriter(JobConf conf, OutputStream fileOut) {
        this.fileOut = fileOut;

        try {
            ignoreDummyColumn = ContextTool.skipDummyColumn(conf);
            String dmlFile = conf.get(Constant.AB_DML_FILE);
            if (StringUtils.isNotBlank(dmlFile))
                recDef = DmlParser.parseDml(ContextTool.getDmlName(dmlFile), ContextTool.getDmlDefine(conf));
            else {
                recDef = DmlParser.parseDml(AbDataDef.DUMMYNAME, conf.get(Constant.AB_DML_STRING));
            }
            LOG.debug(recDef.getAvroSchema());
        } catch (GdrException | IOException e) {
            e.printStackTrace();
            throw new RuntimeException("unable to parse dml.");
        }
    }

    @Override
    public void close(boolean abort) throws IOException {
        fileOut.close();
    }

}
