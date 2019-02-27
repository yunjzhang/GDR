package org.apache.gdr.common.schema.column.typedetail.decimal;

import org.apache.gdr.common.schema.column.AbDecimalColumnDef;
import org.junit.Test;

import static org.junit.Assert.*;

public class DecimalDelimiterLengthScaleHandlerTest {

    @Test
    public void match() {
        DecimalDelimiterLengthScaleHandler pattern = new DecimalDelimiterLengthScaleHandler();
        assertTrue(pattern.match("'\\u0007'.2,maximum_length=18"));
        assertTrue(pattern.match("U'\\u0007'.2,maximum_length=18"));
        assertTrue(pattern.match("'\\007',0,maximum_length=18"));
        assertTrue(pattern.match("'|',0,maximum_length=18, sign_reserved"));
        assertTrue(pattern.match("'|',0, sign_reserved, maximum_length=18"));
        assertTrue(pattern.match("'\\a',0, sign_reserved, maximum_length=18"));
        assertFalse(pattern.match("18,0"));
        assertFalse(pattern.match("18,2"));
        assertFalse(pattern.match(".2"));
        assertFalse(pattern.match(",2"));
        assertFalse(pattern.match("18"));
        assertFalse(pattern.match("'\007'"));
        assertFalse(pattern.match("\"\007\""));
    }

    @Test
    public void parseTypeDetail() {
        DecimalDelimiterLengthScaleHandler pattern = new DecimalDelimiterLengthScaleHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("U'\\u0007'.2,maximum_length=18", def);
        assertEquals(Long.valueOf(18), def.getLength());
        assertEquals(Integer.valueOf(2), def.getScale());
        assertFalse(def.isImplicitDecimal());
        assertEquals(String.valueOf('\007'), def.getDelimiter());
    }

    @Test
    public void parseTypeDetail1_2() {
        DecimalDelimiterLengthScaleHandler pattern = new DecimalDelimiterLengthScaleHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("'\\007',0,maximum_length=18", def);
        assertEquals(Long.valueOf(18), def.getLength());
        assertEquals(Integer.valueOf(0), def.getScale());
        assertTrue(def.isImplicitDecimal());
        assertEquals(String.valueOf('\007'), def.getDelimiter());
    }

    @Test
    public void parseTypeDetail1_3() {
        DecimalDelimiterLengthScaleHandler pattern = new DecimalDelimiterLengthScaleHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("'\\007',3,maximum_length=18", def);
        assertEquals(Long.valueOf(18), def.getLength());
        assertEquals(Integer.valueOf(3), def.getScale());
        assertTrue(def.isImplicitDecimal());
        assertEquals(String.valueOf('\007'), def.getDelimiter());
    }
}