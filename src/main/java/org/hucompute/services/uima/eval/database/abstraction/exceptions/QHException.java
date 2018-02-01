package org.hucompute.services.uima.eval.database.abstraction.exceptions;

public class QHException extends RuntimeException
{
	protected Exception exception;

	public QHException(Exception exception)
	{
		this.exception = exception;
	}

	public Exception getException()
	{
		return this.exception;
	}
}
