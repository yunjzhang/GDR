package org.apache.gdr.mapred;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

public class avroTool {
    static Schema schema;

    @BeforeClass
    public static void init() {
        //File f = new File("src/test/resources/avro/UbiSession.avsc");
        //schema = Schema.parse(f);
    }

    @AfterClass
    public static void tearDown() {
    }

    @Test
    public void testCols() {
        List<Schema> types = new ArrayList<>();
        types.add(Schema.create(Type.NULL));
        types.add(Schema.create(Type.DOUBLE));
        Schema s = Schema.createUnion(types);
        System.out.println(s.getTypes().toString());
        Field f = new Field("endTimestamp", s, null, JsonProperties.NULL_VALUE);
        //schema.getFields().add(f);
    }

    //@Test
    public void test1() throws URISyntaxException, IOException {
        URI uri;
        String filePath = "ftp://yunjzhang$@127.0.0.1/Users/yunjzhang/workspace/git/zeta/ext/src/test/resources/dml/user.dml";

        String[] strs = filePath.split(File.separator);
        uri = new URI(filePath);

        URI uriOnly = new URI(uri.getScheme() + "://" + uri.getHost());
        FileSystem fs = FileSystem.get(uri, new JobConf());

        InputStream dmlfile = fs.open(new Path(filePath));
        StringBuilder sb = new StringBuilder();

        try {
            URL url = new URL(filePath);
            URLConnection conn = url.openConnection();
            InputStream inputStream = conn.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line = null;
            System.out.println("--- START ---");
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            System.out.println("--- END ---");

            inputStream.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
