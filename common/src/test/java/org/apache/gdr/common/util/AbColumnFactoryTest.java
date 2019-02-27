package org.apache.gdr.common.util;

import org.apache.gdr.common.schema.column.*;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class AbColumnFactoryTest {

    @Test
    public void build() {
        assertTrue(AbColumnFactory.build("STRING") instanceof AbStringColumnDef);
        assertTrue(AbColumnFactory.build("DECIMAL") instanceof AbDecimalColumnDef);
        assertTrue(AbColumnFactory.build("INTEGER") instanceof AbNumberColumnDef);
        assertTrue(AbColumnFactory.build("REAL") instanceof AbNumberColumnDef);
        assertTrue(AbColumnFactory.build("NUMBER") instanceof AbNumberColumnDef);
        assertTrue(AbColumnFactory.build("VOID") instanceof AbBytesColumnDef);
        assertTrue(AbColumnFactory.build("DATE") instanceof AbDateColumnDef);
        assertTrue(AbColumnFactory.build("DATETIME") instanceof AbTimeStampColumnDef);
    }
}