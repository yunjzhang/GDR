package org.apache.gdr.common.datatype;

public enum AbTypeGroup {
    NUMBER, STRING, COMPOUND;
    private String name;

    private AbTypeGroup() {
        this.name = this.name().toLowerCase();
    }

    public String getName() {
        return name;
    }
}
