package org.apache.gdr.common.schema.column.typedetail.decimal;

import org.apache.gdr.common.schema.column.AbDecimalColumnDef;
import org.junit.Test;

import static org.junit.Assert.*;

public class DecimalDelimiterLengthHandlerTest {

    @Test
    public void match() {
        DecimalDelimiterLengthHandler pattern = new DecimalDelimiterLengthHandler();
        assertTrue(pattern.match("'\\u0007',maximum_length=18"));
        assertTrue(pattern.match("U'\\u0007',maximum_length=18"));
        assertTrue(pattern.match("'\\007',maximum_length=18"));
        assertTrue(pattern.match("'|',maximum_length=18, sign_reserved"));
        assertTrue(pattern.match("'|', sign_reserved, maximum_length=18"));
        assertTrue(pattern.match("'\\a', sign_reserved, maximum_length=18"));
        assertTrue(pattern.match("U\"\\u0007\", maximum_length=102"));
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
        DecimalDelimiterLengthHandler pattern = new DecimalDelimiterLengthHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("U'\\u0007',maximum_length=18", def);
        assertEquals(Long.valueOf(18), def.getLength());
        assertNull(def.getScale());
        assertFalse(def.isImplicitDecimal());
        assertEquals(String.valueOf('\007'), def.getDelimiter());
    }

    @Test
    public void parseTypeDetail1_2() {
        DecimalDelimiterLengthHandler pattern = new DecimalDelimiterLengthHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("'\\007',maximum_length=18", def);
        assertEquals(Long.valueOf(18), def.getLength());
        assertNull(def.getScale());
        assertFalse(def.isImplicitDecimal());
        assertEquals(String.valueOf('\007'), def.getDelimiter());
    }

    @Test
    public void parseTypeDetail1_3() {
        DecimalDelimiterLengthHandler pattern = new DecimalDelimiterLengthHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("'\\007',maximum_length=18", def);
        assertEquals(Long.valueOf(18), def.getLength());
        assertNull(def.getScale());
        assertFalse(def.isImplicitDecimal());
        assertEquals(String.valueOf('\007'), def.getDelimiter());
    }

    @Test
    public void parseTypeDetail1_4() {
        DecimalDelimiterLengthHandler pattern = new DecimalDelimiterLengthHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("',',maximum_length=18", def);
        assertEquals(Long.valueOf(18), def.getLength());
        assertNull(def.getScale());
        assertFalse(def.isImplicitDecimal());
        assertEquals(String.valueOf(','), def.getDelimiter());
    }

    @Test
    public void parseTypeDetail1_error1() {
        DecimalDelimiterLengthHandler pattern = new DecimalDelimiterLengthHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("U\"\\u0007\", maximum_length=102", def);
        assertEquals(Long.valueOf(102), def.getLength());
        assertNull(def.getScale());
        assertFalse(def.isImplicitDecimal());
        assertEquals(String.valueOf('\007'), def.getDelimiter());
    }
}