package org.apache.gdr.common.util;

import org.apache.gdr.common.exception.GdrRuntimeException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StreamSearcher {
    // An upper bound on pattern length for searching. Results are undefined for
    // longer patterns.
    public static final int MAX_PATTERN_LENGTH = 128;
    static final int MAX_BYTES_ROW = 5 * 1024;
    static final int MAX_BYTE_IN_ARRAY = 1024;
    static final int MAX_RETURN_BYTE = MAX_BYTES_ROW * MAX_BYTE_IN_ARRAY;
    protected byte[] pattern_;
    protected int[] borders_;

    public StreamSearcher(byte[] pattern) {
        setPattern(pattern);
    }

    /**
     * Builds up a table of longest "borders" for each prefix of the pattern to
     * find. This table is stored internally and aids in implementation of the
     * Knuth-Moore-Pratt string search.
     * <p>
     * For more information, see:
     * http://www.inf.fh-flensburg.de/lang/algorithmen/pattern/kmpen.htm.
     */
    protected void preProcess() {
        int i = 0;
        int j = -1;
        borders_[i] = j;
        while (i < pattern_.length) {
            while (j >= 0 && pattern_[i] != pattern_[j]) {
                j = borders_[j];
            }
            borders_[++i] = ++j;
        }
    }

    public void setPattern(byte[] pattern) {
        if (MAX_PATTERN_LENGTH < pattern.length)
            throw new GdrRuntimeException("Search partten longer than limitation.");
        if (pattern == null || pattern.length == 0)
            throw new GdrRuntimeException("Search partten cannot be null.");

        pattern_ = Arrays.copyOf(pattern, pattern.length);
        borders_ = new int[pattern_.length + 1];
        preProcess();
    }

    public byte[] search(InputStream stream) throws IOException {
        int bytesRead = 0;
        List<byte[]> bList = new ArrayList<>();
        byte[] bb = new byte[MAX_BYTE_IN_ARRAY];
        bList.add(bb);

        int b;
        int j = 0;
        int row = 0;

        while ((b = stream.read()) >= 0) {
            bytesRead++;
            if (bytesRead > MAX_RETURN_BYTE)
                throw new GdrRuntimeException("return string exceeds limitation.");

            //bb[bytesRead-1] = (byte) b;
            if (row >= MAX_BYTE_IN_ARRAY) {
                bb = new byte[MAX_BYTE_IN_ARRAY];
                bList.add(bb);
                row = 0;
            }
            bb[row] = (byte) b;

            while (j >= 0 && (byte) b != pattern_[j]) {
                j = borders_[j];
            }
            // Move to the next character in the pattern.
            ++j;

            // If we've matched up to the full pattern length, we found it.
            // Return,
            // which will automatically save our position in the InputStream at
            // the point immediately
            // following the pattern match.
            if (j == pattern_.length) {
                int len = bytesRead - pattern_.length;
                int line = len / MAX_BYTE_IN_ARRAY;
                row = len % MAX_BYTE_IN_ARRAY;
                byte[] out = new byte[len];
                for (int z = 0; z < line; z++) {
                    System.arraycopy(bList.get(z), 0, out, z * MAX_BYTE_IN_ARRAY, MAX_BYTE_IN_ARRAY);
                }
                if (row > 0)
                    System.arraycopy(bList.get(line), 0, out, line * MAX_BYTE_IN_ARRAY, row);
                return out;
            }
            row++;
        }

        return null;
    }
}
