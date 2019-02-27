package org.apache.gdr.common.util;

import java.util.HashSet;
import java.util.Set;

public enum CommentType {
    JAVA, SQL, DML;
    private String name;

    private CommentType() {
        this.name = this.name().toUpperCase();
    }

    public String getName() {
        return name;
    }

    public String getBlockCommentFirstChar() {
        switch (this) {
            case DML:
            case JAVA:
            case SQL:
                return "/";
        }
        throw new AssertionError("Unknown operations " + this);
    }

    public String getBlockCommentSecondChar() {
        switch (this) {
            case DML:
            case JAVA:
            case SQL:
                return "*";
        }
        throw new AssertionError("Unknown operations " + this);
    }

    public String getBlockCommentThirdChar() {
        switch (this) {
            case DML:
            case JAVA:
            case SQL:
                return "*";
        }
        throw new AssertionError("Unknown operations " + this);
    }

    public String getBlockCommentForthChar() {
        switch (this) {
            case DML:
            case JAVA:
            case SQL:
                return "/";
        }
        throw new AssertionError("Unknown operations " + this);
    }

    public String getLineCommentFirstChar() {
        switch (this) {
            case DML:
            case JAVA:
                return "/";
            case SQL:
                return "-";
        }
        throw new AssertionError("Unknown operations " + this);
    }

    public String getLineCommentSecondChar() {
        switch (this) {
            case DML:
            case JAVA:
                return "/";
            case SQL:
                return "-";
        }
        throw new AssertionError("Unknown operations " + this);
    }

    public String getLineDelimiter() {
        switch (this) {
            case DML:
            case JAVA:
            case SQL:
                return "\n";
        }
        throw new AssertionError("Unknown operations " + this);
    }

    public Set<String> getQuotationCharList() {
        Set<String> s = new HashSet<>();
        s.add("'");
        s.add("\"");
        switch (this) {
            case DML:
            case JAVA:
            case SQL:
                return s;
        }
        throw new AssertionError("Unknown operations " + this);
    }
}
