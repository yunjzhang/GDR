package org.apache.gdr.ext.tool.schema;

import org.apache.gdr.common.exception.GdrException;
import org.apache.gdr.common.util.GdrParser;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class GenDmlFromGdr {

    public static void main(String[] args) throws IOException, GdrException {
        if (args.length != 1) {
            System.out.println("Parameter needs: [gdr string] or [gdr file]");
            System.exit(-1);
        }

        File tmpDir = new File(args[0]);
        if (tmpDir.exists())
            System.out.println(GdrParser.parseGdr(new DataInputStream(new FileInputStream(tmpDir))).getGdrSchema());
        else
            System.out.println(GdrParser.parseGdr(args[1]).getGdrSchema());
    }
}
