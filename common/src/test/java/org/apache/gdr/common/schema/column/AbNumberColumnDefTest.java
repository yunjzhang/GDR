package org.apache.gdr.common.schema.column;

import org.apache.gdr.common.datatype.AbDataType;
import org.apache.gdr.common.exception.AbTypeException;
import org.apache.gdr.common.util.AbUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.*;

public class AbNumberColumnDefTest {
    static AbNumberColumnDef col;

    @BeforeClass
    public static void init() {
        col = new AbNumberColumnDef();
        col.setType(AbDataType.INTEGER);
        col.setLength(4l);
        col.setNVL("-99");
        col.setNullable(true);
        col.setName("testC");
    }

    @Test
    public void convertValue4Bytes() throws AbTypeException {
        byte[] b1 = {0, 0, 0, 0};
        assertEquals(0, col.convertValue(b1));
    }

    @Test
    public void convertValue4String() throws AbTypeException {
        assertEquals(123, col.convertValue("123"));
    }

    @Test
    public void readInteger() throws Throwable {
        InputStream targetStream = new ByteArrayInputStream(
                ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(Integer.valueOf(-99)).array());
        assertNull(col.read(targetStream, false));

        targetStream = new ByteArrayInputStream(
                ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(Integer.valueOf(100)).array());
        assertEquals(100, col.read(targetStream, false));
    }

    @Test
    public void write() throws IOException {
        OutputStream os = new ByteArrayOutputStream();
        col.write(os, null, true);
        assertTrue(AbUtils.compareByteArray(
                ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(Integer.valueOf(-99)).array(),
                ((ByteArrayOutputStream) os).toByteArray()));
    }

    @Test
    public void format() {
    }

    @Test
    public void getGdrSchema() {
    }
}