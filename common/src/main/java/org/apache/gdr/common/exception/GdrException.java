package org.apache.gdr.common.exception;

import java.text.ParseException;

public class GdrException extends Exception {
    private static final long serialVersionUID = -2159769297318647450L;

    public GdrException(String string) {
        super(string);
    }

    public GdrException(String str, ParseException e) {
        super(str, e);
    }

    public GdrException(ParseException e) {
        super(e);
    }
}
