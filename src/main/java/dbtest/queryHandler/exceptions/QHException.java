package dbtest.queryHandler.exceptions;

public class QHException extends Exception
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
