package org.apache.gdr.common.schema.column.typedetail.decimal;

public enum DecimalType {
    IMPLICIT("implicit"), EXPLICIT("explicit");

    private String name;

    private DecimalType(String name) {
        this.name = name;
    }

    public static DecimalType fromString(String text) {
        for (DecimalType b : DecimalType.values()) {
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
