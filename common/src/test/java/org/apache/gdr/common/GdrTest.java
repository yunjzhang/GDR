package org.apache.gdr.common;

import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.schema.dataset.AbDataDef;
import org.apache.gdr.common.util.DmlParser;
import org.apache.gdr.common.util.GdrParser;
import junit.framework.Assert;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static junit.framework.Assert.assertTrue;

public class GdrTest {
    static AbDataDef abSchema;
    static AbDataDef abSchema_dml;
    static BufferedReader br;
    static String gdrPath = "target/gdr";
    static String outPath = "target/dat1";
    static File outDir = null;
    static File gdrDir = null;

    static Map<String, String> dmlMap = new HashMap<>();
    static Map<String, String> gdrMap = new HashMap<>();
    static int dmlCols = 0;

    static {

    }

    @BeforeClass
    public static void init() throws IOException, GdrException {
        outDir = new File(outPath);
        if (!outDir.exists())
            outDir.mkdir();

        gdrDir = new File(gdrPath);
        if (!gdrDir.exists())
            gdrDir.mkdir();
    }

    static AbDataDef getDef(String fp) throws IOException, GdrException {
        File f = new File(fp);
        br = new BufferedReader(new FileReader(f));
        StringBuilder sb = new StringBuilder();

        String sCurrentLine;

        while ((sCurrentLine = br.readLine()) != null) {
            sb.append(sCurrentLine).append("\n");
        }

        String name = f.getName().replace(".dml", "");
        String[] strs = name.split("\\.", 3);
        if (strs.length > 1) {
            name = strs[1];
        } else {
            name = strs[0];
        }

        if (br != null)
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        return DmlParser.parseDml(name, sb.toString());
    }

    public static String getDigest(InputStream is, MessageDigest md, int byteArraySize)
            throws NoSuchAlgorithmException, IOException {

        md.reset();
        byte[] bytes = new byte[byteArraySize];
        int numBytes;
        while ((numBytes = is.read(bytes)) != -1) {
            md.update(bytes, 0, numBytes);
        }
        byte[] digest = md.digest();
        String result = new String(Hex.encodeHex(digest));
        return result;
    }

    @Test
    public void TestGdrSchema() throws IOException, GdrException {
        for (Entry<String, String> kv : dmlMap.entrySet()) {
            abSchema_dml = getDef(kv.getKey());
            String gdrFile = getGdrFileName(kv.getKey());
            genGdrFile(abSchema_dml, gdrFile);
            abSchema = GdrParser.parseGdr(gdrFile);
            System.out.println(abSchema.toString());
            System.out.println(abSchema_dml.toString());

            assertTrue(abSchema.equals(abSchema_dml));

        }
    }

    private String getGdrFileName(String key) {
        String filename = StringUtils.substring(key, key.lastIndexOf("/"));
        filename = filename.replace("dml", "gdr");
        return gdrPath + "/" + filename;
    }

    private void genGdrFile(AbDataDef abSchema2, String gdrFile) throws IOException {
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(new File(gdrFile)));
        dos.write(abSchema2.getGdrSchema().toString().getBytes());
        dos.close();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void TestReread() throws IOException, NoSuchAlgorithmException, GdrException {
        for (Entry<String, String> kv : gdrMap.entrySet()) {
            abSchema = GdrParser.parseGdr(kv.getKey());
            DataInputStream fis = new DataInputStream(new FileInputStream(new File(kv.getValue())));

            AbRecord data = null;
            DataOutputStream dout;

            boolean enableNVL = true;
            boolean reGenHead = true;

            String outFile = outDir.getPath() + "/"
                    + kv.getValue().substring(kv.getValue().lastIndexOf("/"), kv.getValue().lastIndexOf("."))
                    + ".out.bin";
            String finalFile = outDir.getPath() + "/"
                    + kv.getValue().substring(kv.getValue().lastIndexOf("/"), kv.getValue().lastIndexOf("."))
                    + ".final.bin";

            dout = new DataOutputStream(new FileOutputStream(new File(outFile)));
            int i = 0;
            try {
                while (fis.available() > 0) {
                    i++;
                    data = new AbRecord(abSchema);
                    data.read(fis);
                    data.write(dout, enableNVL, reGenHead);
                }
            } catch (GdrException e) {
                System.out.println("error on line: " + i);
                e.printStackTrace();
            }

            dout.close();
            fis.close();

            // test output file
            fis = new DataInputStream(new FileInputStream(new File(outFile)));
            dout = new DataOutputStream(new FileOutputStream(new File(finalFile)));
            while (fis.available() > 0) {
                data = new AbRecord(abSchema);
                data.read(fis);
                data.write(dout, enableNVL, reGenHead);
            }

            dout.close();
            fis.close();

            FileInputStream is = new FileInputStream(outFile);
            MessageDigest md = MessageDigest.getInstance("MD5");
            String digOut = getDigest(is, md, 2048);
            is.close();
            is = new FileInputStream(finalFile);
            md = MessageDigest.getInstance("MD5");
            String digFin = getDigest(is, md, 2048);
            is.close();
            is = new FileInputStream(kv.getValue());
            md = MessageDigest.getInstance("MD5");
            String digIn = getDigest(is, md, 2048);
            is.close();

            DataInputStream d1 = null, d0 = null;
            if (!digIn.equals(digOut)) {
                System.out.println("md5 diff between " + kv.getValue() + " and " + outFile + " checking content now:");
                try {
                    d1 = new DataInputStream(new FileInputStream(outFile));
                    d0 = new DataInputStream(new FileInputStream(kv.getValue()));
                    i = 0;
                    while (d0.available() > 0) {
                        i++;
                        String s0 = d0.readLine();
                        String s1 = d1.readLine();
                        //assertEquals("diff on line " + i + ":", s0, s1);

                        if (!s0.equals(s1))
                            System.out.println(Assert.format("diff on line " + i + ":", s0, s1));
                    }
                } catch (IOException e1) {
                    throw e1;
                } finally {
                    if (d1 != null) d1.close();
                    if (d0 != null) d0.close();
                }
            }

            if (!digOut.equals(digFin)) {
                System.out.println("md5 diff between " + outFile + " and " + finalFile + " checking contect now:");
                try {
                    d1 = new DataInputStream(new FileInputStream(outFile));
                    d0 = new DataInputStream(new FileInputStream(finalFile));
                    i = 0;
                    while (d0.available() > 0) {
                        i++;
                        String s0 = d0.readLine();
                        String s1 = d1.readLine();
                        //assertEquals("diff on line " + i + ":", s0, s1);

                        if (!s0.equals(s1))
                            System.out.println(Assert.format("diff on line " + i + ":", s0, s1));
                    }
                } catch (Exception e1) {
                    throw e1;
                } finally {
                    if (d1 != null) d1.close();
                    if (d0 != null) d0.close();
                }
            }
        }
    }

    boolean compareBufferEnd(String delimiter, ByteBuffer bb) {
        byte[] bytes = delimiter.getBytes();
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != bb.get(bb.position() - bytes.length + i)) {
                return false;
            }
        }
        return true;
    }
}
