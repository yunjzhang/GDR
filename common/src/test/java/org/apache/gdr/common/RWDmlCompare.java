package org.apache.gdr.common;

import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.schema.dataset.AbDataDef;
import org.apache.gdr.common.util.DmlParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class RWDmlCompare {
    static Log LOG = LogFactory.getLog(RWDmlCompare.class);

    static AbDataDef abSchema1, abSchema2;
    static BufferedReader br;
    static File testDir;

    static int dmlCols = 0;
    static Map<String, String> inputMap = new HashMap<>();

    @BeforeClass
    public static void init() throws IOException {
        DataInputStream fis = new DataInputStream(new BufferedInputStream(
                new FileInputStream(new File("src/test/resources/dml/rw/read.dml.lst"))));
        int cnt = 0;
        while (fis.available() > 0) {
            String readDml = "src/test/resources/dml/rw/" + fis.readLine();
            //check write/overide dml
            String adpoDml = readDml.replace("read", "read.adpo");
            testDir = new File(adpoDml);
            if (testDir.isFile()) {
                inputMap.put(adpoDml, readDml);
            }
        }

        if (!testDir.exists())
            testDir.mkdir();
    }

    static AbDataDef getDef(String fp) throws IOException, GdrException {
        File f = new File(fp);
        br = new BufferedReader(new FileReader(f));
        StringBuilder sb = new StringBuilder();

        int i;
        while ((i = br.read()) >= 0) {
            sb.append((char) i);
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

    @AfterClass
    public static void tearDown() throws IOException {
    }

    @Test
    public void compareDml() throws IOException, GdrException {
        for (Entry<String, String> kv : inputMap.entrySet()) {
            System.out.println(kv.getKey());
            System.out.println(kv.getValue());
            abSchema1 = getDef(kv.getKey());
            abSchema2 = getDef(kv.getValue());
            if (!abSchema1.equals(abSchema2)) {
                System.out.println(kv.getKey());
                System.out.println(abSchema1.toString());
                System.out.println(abSchema2.toString());
            }
        }
    }
}
