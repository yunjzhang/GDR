package org.apache.gdr.common.datatype;

public enum TdDataType {
    BYTEINT {
        public HiveDataType asHiveType() {
            return HiveDataType.TINYINT;
        }

        public AbDataType asAbType() {
            return AbDataType.INTEGER;
        }
    },
    SMALLINT {
        public HiveDataType asHiveType() {
            return HiveDataType.SMALLINT;
        }

        public AbDataType asAbType() {
            return AbDataType.INTEGER;
        }
    },
    INTEGER {
        public HiveDataType asHiveType() {
            return HiveDataType.INT;
        }

        public AbDataType asAbType() {
            return AbDataType.INTEGER;
        }
    },
    BIGINT {
        public HiveDataType asHiveType() {
            return HiveDataType.BIGINT;
        }

        public AbDataType asAbType() {
            return AbDataType.INTEGER;
        }
    },
    FLOAT {
        public HiveDataType asHiveType() {
            return HiveDataType.FLOAT;
        }

        public AbDataType asAbType() {
            return AbDataType.REAL;
        }
    },
    DOUBLE {
        public HiveDataType asHiveType() {
            return HiveDataType.DOUBLE;
        }

        public AbDataType asAbType() {
            return AbDataType.REAL;
        }
    },
    REAL {
        public HiveDataType asHiveType() {
            return HiveDataType.DOUBLE;
        }

        public AbDataType asAbType() {
            return AbDataType.REAL;
        }
    },
    NUMERIC {
        public HiveDataType asHiveType() {
            return HiveDataType.DECIMAL;
        }

        public AbDataType asAbType() {
            return AbDataType.NUMBER;
        }
    },
    DECIMAL {
        public HiveDataType asHiveType() {
            return HiveDataType.DECIMAL;
        }

        public AbDataType asAbType() {
            return AbDataType.DECIMAL;
        }
    },
    CHAR {
        public HiveDataType asHiveType() {
            return HiveDataType.CHAR;
        }

        public AbDataType asAbType() {
            return AbDataType.STRING;
        }
    },
    VARCHAR {
        public HiveDataType asHiveType() {
            return HiveDataType.VARCHAR;
        }

        public AbDataType asAbType() {
            return AbDataType.STRING;
        }
    },
    TIME {
        public HiveDataType asHiveType() {
            return HiveDataType.TIMESTAMP;
        }

        public AbDataType asAbType() {
            return AbDataType.DATETIME;
        }
    },
    DATE {
        public HiveDataType asHiveType() {
            return HiveDataType.DATE;
        }

        public AbDataType asAbType() {
            return AbDataType.DATE;
        }
    },
    TIMESTAMP {
        public HiveDataType asHiveType() {
            return HiveDataType.TIMESTAMP;
        }

        public AbDataType asAbType() {
            return AbDataType.DATETIME;
        }
    },
    BYTE {
        public HiveDataType asHiveType() {
            return HiveDataType.BINARY;
        }

        public AbDataType asAbType() {
            return AbDataType.VOID;
        }
    },
    VARBYTE {
        public HiveDataType asHiveType() {
            return HiveDataType.BINARY;
        }

        public AbDataType asAbType() {
            return AbDataType.VOID;
        }
    };

    private String name;

    private TdDataType() {
        this.name = this.name().toUpperCase();
    }

    public static Boolean exist(String text) {
        for (TdDataType b : TdDataType.values()) {
            if (b.name.equalsIgnoreCase(text)) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    public String getName() {
        return name;
    }

    public AbDataType asAbType() {
        return null; // default mapping
    }

    public HiveDataType asHiveType() {
        return null; // default mapping
    }
}