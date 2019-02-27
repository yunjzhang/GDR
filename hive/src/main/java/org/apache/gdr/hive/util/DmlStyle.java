package org.apache.gdr.hive.util;

public enum DmlStyle {
    TEXTDELIMITER, BINARY;
    private String name;

    private DmlStyle() {
        this.name = this.name().toUpperCase();
    }

    public String getName() {
        return name;
    }
}
