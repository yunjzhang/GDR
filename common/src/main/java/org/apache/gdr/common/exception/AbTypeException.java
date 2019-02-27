package org.apache.gdr.common.exception;

import java.text.ParseException;

public class AbTypeException extends GdrException {
    private static final long serialVersionUID = -8472677129538829394L;

    public AbTypeException(String string) {
        super(string);
    }

    public AbTypeException(String str, ParseException e) {
        super(str, e);
    }

    public AbTypeException(ParseException e) {
        super(e);
    }
}
