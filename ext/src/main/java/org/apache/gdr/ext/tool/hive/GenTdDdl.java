package org.apache.gdr.ext.tool.hive;

import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.ext.util.HiveTableDefinition;
import org.apache.gdr.ext.util.TdDdlBuilder;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class GenTdDdl {

    public static void main(String[] args) throws IOException, GdrException, SemanticException, ParseException {
        if (args.length != 1) {
            System.out.println("Parameter needs: [ddl string] or [ddl file]");
            System.exit(-1);
        }

        File tmpDir = new File(args[0]);
        String dmlStr = args[0];

        if (tmpDir.exists()) {
            DataInputStream ins = new DataInputStream(new FileInputStream(tmpDir));
            StringBuilder sb = new StringBuilder();
            byte[] b = new byte[1024];
            int len = 0;
            while ((len = ins.read(b)) > 0) {
                sb.append(new String(b, 0, len));
            }
            ins.close();
            dmlStr = sb.toString();
        }

        HiveTableDefinition ctd = new HiveTableDefinition(dmlStr);

        System.out.println(TdDdlBuilder.build(ctd));
    }
}
