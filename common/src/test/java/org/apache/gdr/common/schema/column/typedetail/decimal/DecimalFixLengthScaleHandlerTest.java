package org.apache.gdr.common.schema.column.typedetail.decimal;

import org.apache.gdr.common.schema.column.AbDecimalColumnDef;
import org.junit.Test;

import static org.junit.Assert.*;

public class DecimalFixLengthScaleHandlerTest {

    @Test
    public void match() {
        DecimalFixLengthScaleHandler pattern = new DecimalFixLengthScaleHandler();
        assertTrue(pattern.match("18,0"));
        assertTrue(pattern.match("18,2"));
        assertFalse(pattern.match(".2"));
        assertFalse(pattern.match(",2"));
        assertFalse(pattern.match("18"));
        assertFalse(pattern.match("'\007'"));
        assertFalse(pattern.match("\"\007\""));
        assertTrue(pattern.match("39,0, sign_reserved"));
    }

    @Test
    public void parseTypeDetail() {
        DecimalFixLengthScaleHandler pattern = new DecimalFixLengthScaleHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("18,2", def);
        assertEquals(Long.valueOf(18), def.getLength());
        assertEquals(Integer.valueOf(2), def.getScale());
        assertTrue(def.isImplicitDecimal());
    }

    @Test
    public void parseTypeDetail1_2() {
        DecimalFixLengthScaleHandler pattern = new DecimalFixLengthScaleHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("19.2", def);
        assertEquals(Long.valueOf(19), def.getLength());
        assertEquals(Integer.valueOf(2), def.getScale());
        assertFalse(def.isImplicitDecimal());
    }
}