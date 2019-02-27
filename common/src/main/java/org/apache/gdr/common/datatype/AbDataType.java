package org.apache.gdr.common.datatype;

public enum AbDataType {
    STRING {
        public HiveDataType asHiveType() {
            return HiveDataType.STRING;
        }

        public TdDataType asTdType() {
            return TdDataType.VARCHAR;
        }
    },
    DECIMAL {
        public HiveDataType asHiveType() {
            return HiveDataType.DECIMAL;
        }

        public TdDataType asTdType() {
            return TdDataType.DECIMAL;
        }
    },
    INTEGER {
        public HiveDataType asHiveType() {
            return HiveDataType.INT;
        }

        public TdDataType asTdType() {
            return TdDataType.INTEGER;
        }
    },
    REAL {
        public HiveDataType asHiveType() {
            return HiveDataType.DOUBLE;
        }

        public TdDataType asTdType() {
            return TdDataType.FLOAT;
        }
    },
    VOID {
        public HiveDataType asHiveType() {
            return HiveDataType.TINYINT;
        }

        public TdDataType asTdType() {
            return TdDataType.INTEGER;
        }
    },
    DATE {
        public HiveDataType asHiveType() {
            return HiveDataType.TINYINT;
        }

        public TdDataType asTdType() {
            return TdDataType.INTEGER;
        }
    },
    DATETIME {
        public HiveDataType asHiveType() {
            return HiveDataType.TINYINT;
        }

        public TdDataType asTdType() {
            return TdDataType.INTEGER;
        }
    },
    RECORD {
        public HiveDataType asHiveType() {
            return HiveDataType.TINYINT;
        }

        public TdDataType asTdType() {
            return TdDataType.INTEGER;
        }
    },
    UNION {
        public HiveDataType asHiveType() {
            return null;
        }

        public TdDataType asTdType() {
            return null;
        }
    },
    VECTOR {
        public HiveDataType asHiveType() {
            return null;
        }

        public TdDataType asTdType() {
            return null;
        }
    },
    NUMBER {
        public HiveDataType asHiveType() {
            return HiveDataType.DECIMAL;
        }

        public TdDataType asTdType() {
            return TdDataType.NUMERIC;
        }
    };
    private String name;

    private AbDataType() {
        this.name = this.name().toUpperCase();
    }

    public static Boolean exist(String text) {
        for (AbDataType b : AbDataType.values()) {
            if (b.name.equalsIgnoreCase(text)) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    public String getName() {
        return name;
    }

    public TdDataType asTdType() {
        return null; // default mapping
    }

    public HiveDataType asHiveType() {
        return null; // default mapping
    }
}