package org.apache.gdr.common.datatype;

public enum HiveDataType {
    TINYINT {
        public TdDataType asTdType() {
            return TdDataType.BYTEINT;
        }

        public AbDataType asAbType() {
            return AbDataType.INTEGER;
        }
    },
    SMALLINT {
        public TdDataType asTdType() {
            return TdDataType.SMALLINT;
        }

        public AbDataType asAbType() {
            return AbDataType.INTEGER;
        }
    },
    INT {
        public TdDataType asTdType() {
            return TdDataType.INTEGER;
        }

        public AbDataType asAbType() {
            return AbDataType.INTEGER;
        }
    },
    BIGINT {
        public TdDataType asTdType() {
            return TdDataType.BIGINT;
        }

        public AbDataType asAbType() {
            return AbDataType.INTEGER;
        }
    },
    FLOAT {
        public TdDataType asTdType() {
            return TdDataType.FLOAT;
        }

        public AbDataType asAbType() {
            return AbDataType.DECIMAL;
        }
    },
    DOUBLE {
        public TdDataType asTdType() {
            return TdDataType.DOUBLE;
        }

        public AbDataType asAbType() {
            return AbDataType.REAL;
        }
    },
    DECIMAL {
        public TdDataType asTdType() {
            return TdDataType.DECIMAL;
        }

        public AbDataType asAbType() {
            return AbDataType.DECIMAL;
        }
    },
    CHAR {
        public TdDataType asTdType() {
            return TdDataType.CHAR;
        }

        public AbDataType asAbType() {
            return AbDataType.STRING;
        }
    },
    VARCHAR {
        public TdDataType asTdType() {
            return TdDataType.VARCHAR;
        }

        public AbDataType asAbType() {
            return AbDataType.STRING;
        }
    },
    STRING {
        public TdDataType asTdType() {
            return TdDataType.VARCHAR;
        }

        public AbDataType asAbType() {
            return AbDataType.STRING;
        }
    },
    DATE {
        public TdDataType asTdType() {
            return TdDataType.DATE;
        }

        public AbDataType asAbType() {
            return AbDataType.DATE;
        }
    },
    TIMESTAMP {
        public TdDataType asTdType() {
            return TdDataType.TIMESTAMP;
        }

        public AbDataType asAbType() {
            return AbDataType.DATETIME;
        }
    },
    BOOLEAN {
        public TdDataType asTdType() {
            return TdDataType.BYTEINT;
        }

        public AbDataType asAbType() {
            return AbDataType.INTEGER;
        }
    },
    BINARY {
        public TdDataType asTdType() {
            return TdDataType.VARBYTE;
        }

        public AbDataType asAbType() {
            return AbDataType.VOID;
        }
    };

    private String name;

    private HiveDataType() {
        this.name = this.name().toUpperCase();
    }

    public static Boolean exist(String text) {
        for (HiveDataType b : HiveDataType.values()) {
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

    public AbDataType asAbType() {
        return null; // default mapping
    }
}