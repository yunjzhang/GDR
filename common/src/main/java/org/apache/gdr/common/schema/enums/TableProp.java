package org.apache.gdr.common.schema.enums;

public enum TableProp {
    NAME("name"), NAMESPACE("namespace"), DOC("doc"), HIDENULL("hidenull"), FIELDS("fields");

    private String name;

    private TableProp(String name) {
        this.name = name;
    }

    public static TableProp fromString(String text) {
        for (TableProp b : TableProp.values()) {
            if (b.name.equalsIgnoreCase(text)) {
                return b;
            }
        }
        return null;
    }

    public String getName() {
        return name;
    }
}
