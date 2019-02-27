package org.apache.gdr.ext.util;

import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.schema.column.AbColumnDef;
import org.apache.gdr.common.schema.dataset.AbDataDef;
import org.apache.gdr.hive.util.DmlStyle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.List;

public class AbDataDefBuilder {
    static Log LOG = LogFactory.getLog(AbDataDefBuilder.class);

    public static AbDataDef build(HiveTableDefinition ctd) throws GdrException {
        String fd = ctd.getFieldDelim();
        String ld = ctd.getLineDelim();
        DmlStyle type = DmlStyle.TEXTDELIMITER;

        List<FieldSchema> columnList = ctd.getCols();

        AbDataDef dataset = new AbDataDef(ctd.getTableName());
        //dataset.setCharset(StandardCharsets.ISO_8859_1);
        //last column uses line delimiter as field delimiter
        int colCount = columnList.size();
        int i = 0;
        for (FieldSchema fs : columnList) {
            i++;
            try {
                AbColumnDef col = (i == colCount) ?
                        AbColumnDefBuilder.build(fs, type, ld, "\\N") : AbColumnDefBuilder.build(fs, type, fd, "\\N");
                LOG.debug(col);
                dataset.addColumn(col);
            } catch (GdrException e) {
                LOG.error("ERROR: bad col: " + fs);
                throw e;
            }
        }

        return dataset;
    }
}
