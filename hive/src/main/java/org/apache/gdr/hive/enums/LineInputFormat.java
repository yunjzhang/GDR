package org.apache.gdr.hive.enums;

;

public enum LineInputFormat {
    TextInputFormat("TextInputFormat"), CsvInputFormat("CsvInputFormat");

    private String name;

    private LineInputFormat(String name) {
        this.name = name;
    }

    public static LineInputFormat fromString(String text) {
        for (LineInputFormat b : LineInputFormat.values()) {
            if (b.name.equalsIgnoreCase(text)) {
                return b;
            }
        }
        return null;
    }

    public static Boolean exist(String text) {
        for (LineInputFormat b : LineInputFormat.values()) {
            if (b.name.equalsIgnoreCase(text)) {
                return true;
            }
        }
        return false;
    }

    public String getName() {
        return name;
    }
}
