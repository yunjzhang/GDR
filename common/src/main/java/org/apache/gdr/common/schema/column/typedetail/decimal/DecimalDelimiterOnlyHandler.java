package org.apache.gdr.common.schema.column.typedetail.decimal;

import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.schema.column.AbColumnDef;
import org.apache.gdr.common.schema.column.AbDecimalColumnDef;
import org.apache.gdr.common.util.AbUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DecimalDelimiterOnlyHandler implements TypeDetailInterface {
    static final Log LOG = LogFactory.getLog(DecimalDelimiterOnlyHandler.class);

    //('u\0007',0,maximum_length=18)
    //('\007'.2,maximum_length=18)
    static final String patterns[] =
            {
                    "^U?(['\"])(\\\\[uU][0-9A-F]{1,4})\\1",
                    "^(['\"])(\\\\[xX][0-9A-F]{2})\\1",
                    "^(['\"])(\\\\[0-7]{3})\\1",
                    "^U?(['\"])(\\\\[abfnrtv])\\1",
                    "^(['\"])(.{1,3})\\1"
            };

    @Override
    public Boolean match(String detail) {
        if (detail == null)
            return Boolean.FALSE;
        else
            return detail.matches(patterns[0]) ||
                    detail.matches(patterns[1]) ||
                    detail.matches(patterns[2]) ||
                    detail.matches(patterns[3]) ||
                    detail.matches(patterns[4]);
    }

    @Override
    public void parseTypeDetail(String detail, AbColumnDef abColumnDef) {
        if (StringUtils.isBlank(detail))
            throw new GdrRuntimeException("no data type definition reads from input.");

        if (!(abColumnDef instanceof AbDecimalColumnDef))
            throw new GdrRuntimeException("Only AbDecimalColumnDef is not supported by " + this.getClass().getSimpleName());

        Boolean found = Boolean.FALSE;
        Pattern pattern;
        Matcher matcher = null;
        for (String str : patterns) {
            pattern = Pattern.compile(str);
            matcher = pattern.matcher(detail);
            if (matcher.matches()) {
                found = Boolean.TRUE;
                break;
            }
        }

        if (found) {
            String del = matcher.group(2);
            abColumnDef.setDelimiter(AbUtils.decodeString(del));
        } else {
            throw new GdrRuntimeException("unsupported Decimal pattern: " + detail);
        }
    }
}
