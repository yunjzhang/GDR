package org.apache.gdr.ext.tool.schema;

import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.util.DmlParser;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class GenGdrFromDml {

    public static void main(String[] args) throws IOException, GdrException {
        if (args.length != 1) {
            System.out.println("Parameter needs: [ddl string] or [ddl file]");
            System.exit(-1);
        }

        File tmpDir = new File(args[0]);
        if (tmpDir.exists())
            System.out.println(DmlParser.parseDml(new DataInputStream(new FileInputStream(tmpDir))).getGdrSchema());
        else
            System.out.println(DmlParser.parseDml(args[1]).getGdrSchema());
    }
}
