package org.hucompute.services.uima.eval.utility.logging;

import org.json.JSONObject;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

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

	public static void logJSONObject(Logger logger, JSONObject object)
	{
		for (String key : object.keySet())
		{
			Object value = object.get(key);
			if (value instanceof JSONObject)
			{
				logger.fine(key + " - {");
				logJSONObject(logger, (JSONObject) value);
				logger.fine("} - " + key);
			} else {
				logger.fine(key + " - " + value.toString());
			}
		}
	}
}
