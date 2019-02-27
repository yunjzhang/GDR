package org.apache.gdr.common.schema.column.typedetail.decimal;

import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.gdr.common.schema.column.AbColumnDef;
import org.apache.gdr.common.schema.column.AbDecimalColumnDef;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DecimalFixLengthHandler implements TypeDetailInterface {
    static final Log LOG = LogFactory.getLog(DecimalFixLengthHandler.class);

    //(18)
    String pattern = "^([0-9]{1,3}) *([,\\.] *[^0-9].*)*";

    @Override
    public Boolean match(String detail) {
        if (detail == null)
            return Boolean.FALSE;
        else
            return detail.matches(this.pattern);
    }

    @Override
    public void parseTypeDetail(String detail, AbColumnDef abColumnDef) {
        if (StringUtils.isBlank(detail))
            throw new GdrRuntimeException("no data type definition reads from input.");

        if (!(abColumnDef instanceof AbDecimalColumnDef))
            throw new GdrRuntimeException("Only AbDecimalColumnDef is not supported by " + this.getClass().getSimpleName());

        Pattern p = Pattern.compile(pattern);
        Matcher matcher = p.matcher(detail);

        if (matcher.matches()) {
            Long length = Long.valueOf(matcher.group(1));
            if (length.compareTo(38l) > 0)
                LOG.warn(abColumnDef.getName() + ": Decimal length[" + length + "] is defined more than 38");
            abColumnDef.setLength(length);
        } else {
            throw new GdrRuntimeException("unsupported Decimal pattern: " + detail);
        }
    }
}
