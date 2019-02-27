package org.apache.gdr.common.schema.column.typedetail.decimal;

import org.apache.gdr.common.schema.column.AbDecimalColumnDef;
import org.junit.Test;

import static org.junit.Assert.*;

public class DecimalDelimiterOnlyHandlerTest {

    @Test
    public void match() {
        DecimalDelimiterOnlyHandler pattern = new DecimalDelimiterOnlyHandler();
        assertTrue(pattern.match("'\\x01'")); //char(07)
        assertTrue(pattern.match("'\007'")); //char(07)
        assertTrue(pattern.match("'\\007'")); //string \007
        assertTrue(pattern.match("'\\u0007'")); //string \u0007
        assertTrue(pattern.match("U'\\u0007'")); //string U\u0007
        assertTrue(pattern.match("'|'"));  //char |
        assertTrue(pattern.match("'\\a'"));  //char \a
        assertFalse(pattern.match("'u\\0007'.2,maximum_length=18"));
        assertFalse(pattern.match("18,0"));
        assertFalse(pattern.match("18,2"));
        assertFalse(pattern.match(".2"));
        assertFalse(pattern.match(",2"));
        assertFalse(pattern.match("18"));
    }

    @Test
    public void parseTypeDetail() {
        DecimalDelimiterOnlyHandler pattern = new DecimalDelimiterOnlyHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("U'\\u0007'", def);
        assertEquals(String.valueOf('\007'), def.getDelimiter());
        assertTrue(def.getLength().equals(-1l));
    }

    @Test
    public void parseTypeDetail1_2() {
        DecimalDelimiterOnlyHandler pattern = new DecimalDelimiterOnlyHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("'\\007'", def);
        assertEquals(String.valueOf('\007'), def.getDelimiter());
        assertTrue(def.getLength().equals(-1l));
    }

    @Test
    public void parseTypeDetail1_3() {
        DecimalDelimiterOnlyHandler pattern = new DecimalDelimiterOnlyHandler();
        AbDecimalColumnDef def = new AbDecimalColumnDef();
        pattern.parseTypeDetail("'\007'", def);
        assertEquals(String.valueOf('\007'), def.getDelimiter());
        assertTrue(def.getLength().equals(-1l));
    }
}