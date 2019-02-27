package org.apache.gdr.common.schema.enums;

public enum ColumnProp {
    NAME("name"), NAMESPACE("namespace"), DOC("doc"), TYPE("type"), LENGTH("length"), NULLABLE("nullable"),
    NVL("nvl"), BYTEHEAD("binaryHeader"), UNSIGNHRADERLEN("unsignHeaderLength"), HEADERENDIAN("headerEndian"),
    DELIMITER("delimiter"), FIXVALUE("fixValue"), COMMENT("comment"), ENABLEOUTPUT("output"),
    DATETIME_FORMAT("datetime_format"), SCALE("scale"), UNSIGN_VALUE("unsign_value"),
    VALUE_ENDIAN("value_endian"), CHARSET("charset"), DECIMAL_TYPE("decimal_type");

    private String name;

    private ColumnProp(String name) {
        this.name = name;
    }

    public static ColumnProp fromString(String text) {
        for (ColumnProp b : ColumnProp.values()) {
            if (b.name.equalsIgnoreCase(text)) {
                return b;
            }
        }
        return null;
    }

    public String getName() {
        return name;
    }

    public final boolean equals(String other) {
        return this.getName().equals(other);
    }
}
