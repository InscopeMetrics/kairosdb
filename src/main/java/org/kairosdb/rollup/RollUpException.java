package org.kairosdb.rollup;

import org.kairosdb.core.exception.KairosDBException;

public class RollUpException extends KairosDBException {
    private static final long serialVersionUID = 6722888025886152791L;

    public RollUpException(final String message) {
        super(message);
    }

    public RollUpException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public RollUpException(final Throwable cause) {
        super(cause);
    }
}
