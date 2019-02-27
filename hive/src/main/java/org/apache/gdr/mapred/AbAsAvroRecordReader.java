package org.apache.gdr.mapred;

import org.apache.gdr.common.AbRecord;
import org.apache.gdr.common.exception.AbTypeException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.rmi.server.UID;
import java.util.Date;
import java.util.Random;

public class AbAsAvroRecordReader extends AbRecordReader<NullWritable, AvroGenericRecordWritable> {
    static final Log LOG = LogFactory.getLog(AbAsAvroRecordReader.class);
    UID uid;

    public AbAsAvroRecordReader(JobConf conf, FileSplit split) throws IOException {
        super(conf, split);
        try {
            LOG.debug("Avro DETAIL:" + reader.getAvroSchema());
        } catch (AbTypeException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        Random r = new Random((new Date()).getTime());
        uid = new UID((short) r.nextInt(100));
    }

    @Override
    public boolean next(NullWritable key, AvroGenericRecordWritable value) throws IOException {
        if (getFilePosition() <= stop) {
            GenericRecord rec;
            try {
                AbRecord ab = reader.readRecord(abFileIn);
                if (ab == null)
                    return false;
                rec = ab.toGenericRecord(ignoreDummyColumn, true);
                pos += ab.getBytesRead();
                if (rec != null) {
                    GenericData.Record r = (GenericData.Record) rec;
                    value.setRecord(r);
                    value.setRecordReaderID(uid);
                    value.setFileSchema(rec.getSchema());
                    return true;
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("record read error.");
            }
        }

        return false;
    }

    @Override
    public NullWritable createKey() {
        return NullWritable.get();
    }

    @Override
    public AvroGenericRecordWritable createValue() {
        return new AvroGenericRecordWritable();
    }
}
