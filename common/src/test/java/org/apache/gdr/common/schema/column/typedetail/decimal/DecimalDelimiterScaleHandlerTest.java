package org.apache.gdr.common.schema.column.typedetail.decimal;

import org.apache.gdr.common.schema.column.AbDecimalColumnDef;
import org.junit.Test;

import static org.junit.Assert.*;

public class DecimalDelimiterScaleHandlerTest {

    @Test
    public void match() {
        DecimalDelimiterScaleHandler pattern = new DecimalDelimiterScaleHandler();
        assertTrue(pattern.match("'\\u0007'.2"));
        assertTrue(pattern.match("U'\\u0007',2"));
        assertTrue(pattern.match("'\\007'.2"));
        assertTrue(pattern.match("'|',0"));
        assertTrue(pattern.match("'|', 0"));
        assertTrue(pattern.match("'\\a', 0"));
        assertTrue(pattern.match("\"\\a\",0"));
        // assertFalse(pattern.match("'\\a',0, sign_reserved, maximum_length=18"));
        assertFalse(pattern.match("'\\a',0, maximum_length=18"));
        assertFalse(pattern.match("18,0"));
        assertFalse(pattern.match("18,2"));
        assertFalse(pattern.match(".2"));
        assertFalse(pattern.match(",2"));
        assertFalse(pattern.match("18"));
        assertFalse(pattern.match("'\007'"));
        assertFalse(pattern.match("\"\007\""));
    }

    @Test
    public void parseTypeDetail1_1() {
        DecimalDelimiterScaleHandler pattern = new DecimalDelimiterScaleHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("U'\\u0007'.2", def);
        assertEquals(Long.valueOf(-1), def.getLength());
        assertEquals(Integer.valueOf(2), def.getScale());
        assertFalse(def.isImplicitDecimal());
        assertEquals(String.valueOf('\007'), def.getDelimiter());
    }

    @Test
    public void parseTypeDetail1_2() {
        DecimalDelimiterScaleHandler pattern = new DecimalDelimiterScaleHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("'\\007',2", def);
        assertEquals(Long.valueOf(-1), def.getLength());
        assertEquals(Integer.valueOf(2), def.getScale());
        assertTrue(def.isImplicitDecimal());
        assertEquals(String.valueOf('\007'), def.getDelimiter());
    }
}