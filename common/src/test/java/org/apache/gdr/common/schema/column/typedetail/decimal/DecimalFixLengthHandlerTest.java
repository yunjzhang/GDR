package org.apache.gdr.common.schema.column.typedetail.decimal;

import org.apache.gdr.common.schema.column.AbDecimalColumnDef;
import org.junit.Test;

import static org.junit.Assert.*;

public class DecimalFixLengthHandlerTest {

    @Test
    public void match() {
        DecimalFixLengthHandler pattern = new DecimalFixLengthHandler();
        assertTrue(pattern.match("18"));
        assertFalse(pattern.match("18,0"));
        assertFalse(pattern.match(".2"));
        assertFalse(pattern.match(",2"));
        assertFalse(pattern.match("'\007'"));
        assertFalse(pattern.match("\"\007\""));
    }

    @Test
    public void parseTypeDetail() {
        DecimalFixLengthHandler pattern = new DecimalFixLengthHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("18", def);
        assertEquals(Long.valueOf(18), def.getLength());
        assertNull(def.getScale());
    }

}