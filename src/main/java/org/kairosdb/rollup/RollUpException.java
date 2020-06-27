package org.kairosdb.rollup;

import org.kairosdb.core.exception.KairosDBException;

public class RollUpException extends KairosDBException
{
	private static final long serialVersionUID = 6722888025886152791L;

	public RollUpException(String message)
	{
		super(message);
	}

	public RollUpException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public RollUpException(Throwable cause)
	{
		super(cause);
	}
}
