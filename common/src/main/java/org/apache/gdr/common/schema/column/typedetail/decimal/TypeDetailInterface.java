package org.apache.gdr.common.schema.column.typedetail.decimal;

import org.apache.gdr.common.schema.column.AbColumnDef;

public abstract interface TypeDetailInterface {
    public Boolean match(String pattern);

    public void parseTypeDetail(String detail, AbColumnDef def);
}
