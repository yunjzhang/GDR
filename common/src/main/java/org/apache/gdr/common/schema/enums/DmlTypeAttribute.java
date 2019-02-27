package org.apache.gdr.common.schema.enums;

public enum DmlTypeAttribute {
    CLEAN("clean"), CHARSET("charset"), MAXIMUMLENGTH("maximum_length"), DECIMALPOINT("decimal_point"), SIGNEXPLICIT("sign_explicit"),
    SIGNRESERVED("sign_reserved"), GROUPING("grouping"), GROUPINGSEP("grouping_sep"), ALLOWEXPONENT("allow_exponent");

    private String name;

    private DmlTypeAttribute(String name) {
        this.name = name;
    }

    public static DmlTypeAttribute fromString(String text) {
        for (DmlTypeAttribute b : DmlTypeAttribute.values()) {
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
