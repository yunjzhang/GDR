package org.apache.gdr.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

@UDFType(stateful = true)
public class StringToEpochTimestamp extends UDF {
    long timestamp;

    public Long evaluate(String dateFormatted) throws ParseException {
        return evaluate(dateFormatted, "yyyy-MM-dd HH:mm:ss.SSS");
    }

    public Long evaluate(String dateFormatted, String format) throws ParseException {
        if (StringUtils.isBlank(dateFormatted))
            return null;
        DateFormat formatter = new SimpleDateFormat(format);
        timestamp = formatter.parse(dateFormatted).getTime();
        return timestamp;
    }
}