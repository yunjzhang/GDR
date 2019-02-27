package org.apache.gdr.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@UDFType(stateful = true)
public class EpochDateToString extends UDF {
    String dateFormatted;

    public String evaluate(Long timestamp, String format) {
        if (timestamp == null)
            return null;

        Date date = new Date(timestamp);
        DateFormat formatter = new SimpleDateFormat(format);
        dateFormatted = formatter.format(date);
        return dateFormatted;
    }

    public String evaluate(Long timestamp) {
        return evaluate(timestamp, "yyyy-MM-dd");
    }
}