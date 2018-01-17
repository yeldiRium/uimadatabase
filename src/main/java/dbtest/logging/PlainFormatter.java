package dbtest.logging;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * Formats a LogRecord as a simple "[level] - message".
 */
public class PlainFormatter extends Formatter
{
	@Override
	public String format(LogRecord logRecord)
	{
		return String.format(
				"[%s] %s%n",
				logRecord.getLevel(),
				logRecord.getMessage()
		);
	}
}
