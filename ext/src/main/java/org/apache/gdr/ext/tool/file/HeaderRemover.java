package org.apache.gdr.ext.tool.file;

import org.apache.gdr.common.exception.GdrException;

import java.io.*;

public class HeaderRemover {
    public static void main(String[] args) throws IOException, GdrException {
        if (args.length != 2) {
            printUsage();
        }

        BufferedReader br = new BufferedReader(new FileReader(args[0]));
        BufferedWriter bw = new BufferedWriter(new FileWriter(args[1]));

        int i;
        int n = 10;
        int cnt = 0;
        while (true) {
            i = br.read();
            if (i == -1) break;
            else {
                if (cnt >= 2) bw.write(i);
                cnt++;
                if (i == n) {
                    cnt = 0;
                }
            }
        }

        br.close();
        bw.close();
    }

    private static void printUsage() {
        System.out.println("Usage: HeaderRemover <input file> <output file>");
    }
}
