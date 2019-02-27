package org.apache.gdr.common.util;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class CommentTypeTest {

    @Test
    public void getBlockCommentFirstChar() {
        assertTrue(CommentType.DML.getBlockCommentFirstChar().equals("/"));
        assertTrue(CommentType.JAVA.getBlockCommentFirstChar().equals("/"));
        assertTrue(CommentType.SQL.getBlockCommentFirstChar().equals("/"));
    }

    @Test
    public void getBlockCommentSecondChar() {
        assertTrue(CommentType.DML.getBlockCommentSecondChar().equals("*"));
        assertTrue(CommentType.JAVA.getBlockCommentSecondChar().equals("*"));
        assertTrue(CommentType.SQL.getBlockCommentSecondChar().equals("*"));
    }

    @Test
    public void getBlockCommentThirdChar() {
        assertTrue(CommentType.DML.getBlockCommentThirdChar().equals("*"));
        assertTrue(CommentType.JAVA.getBlockCommentThirdChar().equals("*"));
        assertTrue(CommentType.SQL.getBlockCommentThirdChar().equals("*"));
    }

    @Test
    public void getBlockCommentForthChar() {
        assertTrue(CommentType.DML.getBlockCommentFirstChar().equals("/"));
        assertTrue(CommentType.JAVA.getBlockCommentFirstChar().equals("/"));
        assertTrue(CommentType.SQL.getBlockCommentFirstChar().equals("/"));
    }

    @Test
    public void getLineCommentFirstChar() {
        assertTrue(CommentType.DML.getLineCommentFirstChar().equals("/"));
        assertTrue(CommentType.JAVA.getLineCommentFirstChar().equals("/"));
        assertTrue(CommentType.SQL.getLineCommentFirstChar().equals("-"));
    }

    @Test
    public void getLineCommentSecondChar() {
        assertTrue(CommentType.DML.getLineCommentFirstChar().equals("/"));
        assertTrue(CommentType.JAVA.getLineCommentFirstChar().equals("/"));
        assertTrue(CommentType.SQL.getLineCommentFirstChar().equals("-"));
    }

    @Test
    public void getLineDelimiter() {
        assertTrue(CommentType.DML.getLineDelimiter().equals("\n"));
        assertTrue(CommentType.JAVA.getLineDelimiter().equals("\n"));
        assertTrue(CommentType.SQL.getLineDelimiter().equals("\n"));
    }

    @Test
    public void getQuotationCharList() {
        Set<String> s = new HashSet<>();
        s.add("'");
        s.add("\"");
        assertTrue(CommentType.DML.getQuotationCharList().equals(s));
        assertTrue(CommentType.JAVA.getQuotationCharList().equals(s));
        assertTrue(CommentType.SQL.getQuotationCharList().equals(s));
    }
}