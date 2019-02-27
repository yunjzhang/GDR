package org.apache.gdr.common.exception;

import java.text.ParseException;

public class GdrRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 2658945034186692475L;

    public GdrRuntimeException(String string) {
        super(string);
    }

    public GdrRuntimeException(String str, ParseException e) {
        super(str, e);
    }

    public GdrRuntimeException(ParseException e) {
        super(e);
    }
}
